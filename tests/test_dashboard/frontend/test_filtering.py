from unittest import TestCase

from sds_utils.dashboard.frontend.filtering import (
    is_valid_regex,
    matches_regex,
    matches_search_expression,
    matches_tag_search_expression,
)


class SearchExpressionTests(TestCase):
    def test_wildcard(self) -> None:
        self.assertTrue(matches_search_expression("alpha_middle_omega", "alpha*omega"))
        self.assertFalse(matches_search_expression("alpha_middle_beta", "alpha*omega"))

    def test_double_wildcard_does_not_cross_underscore(self) -> None:
        self.assertTrue(matches_search_expression("alphaMiddleomega", "alpha**omega"))
        self.assertTrue(matches_search_expression("alphaomega", "alpha**omega"))
        self.assertFalse(
            matches_search_expression("alpha_middle_omega", "alpha**omega")
        )
        self.assertTrue(
            matches_search_expression("alpha_middle_omega", "^alpha_**_omega$")
        )

    def test_start_and_end_anchors(self) -> None:
        self.assertTrue(matches_search_expression("alpha_beta", "^alpha"))
        self.assertFalse(matches_search_expression("xalpha_beta", "^alpha"))
        self.assertTrue(matches_search_expression("alpha_beta", "beta$"))
        self.assertTrue(matches_search_expression("alpha_beta", "^alpha_beta$"))
        self.assertFalse(matches_search_expression("xalpha_beta", "^alpha_beta$"))

    def test_negation(self) -> None:
        self.assertTrue(matches_search_expression("alpha_beta", "-gamma"))
        self.assertFalse(matches_search_expression("alpha_beta", "-beta"))

    def test_matching_spans_newlines(self) -> None:
        value = "Not enough information to process.\nMissing ultra_l1a in range"
        self.assertTrue(matches_search_expression(value, "ultra"))
        self.assertFalse(matches_search_expression(value, "-ultra"))

    def test_spaces_combine_terms_with_and(self) -> None:
        self.assertTrue(matches_search_expression("alpha_beta", "alpha beta"))
        self.assertTrue(matches_search_expression("alpha_beta", "^alpha -gamma beta$"))
        self.assertFalse(matches_search_expression("alpha_beta", "alpha gamma"))

    def test_matching_is_case_insensitive(self) -> None:
        self.assertTrue(matches_search_expression("Alpha_Beta", "alpha*BETA"))

    def test_empty_expression_matches(self) -> None:
        self.assertTrue(matches_search_expression("anything", ""))

    def test_anchored_empty_pattern(self) -> None:
        self.assertTrue(matches_search_expression("", "^$"))
        self.assertFalse(matches_search_expression("anything", "^$"))
        self.assertFalse(matches_search_expression("", "-^$"))
        self.assertTrue(matches_search_expression("anything", "-^$"))

    def test_tag_terms_match_individual_tags(self) -> None:
        tags = "needs-review; source_missing; important"
        self.assertTrue(matches_tag_search_expression(tags, "^needs important$"))
        self.assertTrue(matches_tag_search_expression(tags, "^source_**$"))
        self.assertFalse(matches_tag_search_expression(tags, "needs$ important$"))
        self.assertFalse(matches_tag_search_expression(tags, "^review important"))

    def test_excluded_tag_term_rejects_if_any_tag_matches(self) -> None:
        tags = "reviewed; important"
        self.assertFalse(matches_tag_search_expression(tags, "-important"))
        self.assertTrue(matches_tag_search_expression(tags, "-blocked"))

    def test_positive_tag_term_does_not_match_an_empty_tag_list(self) -> None:
        self.assertFalse(matches_tag_search_expression("", "important"))
        self.assertTrue(matches_tag_search_expression("", "-important"))

    def test_regex_has_standard_python_semantics(self) -> None:
        self.assertTrue(matches_regex("alpha_123", r"^alpha_\d+$"))
        self.assertFalse(matches_regex("Alpha_123", r"^alpha_\d+$"))
        self.assertTrue(matches_regex("prefix alpha_123 suffix", r"alpha_\d+"))

    def test_invalid_regex(self) -> None:
        self.assertFalse(is_valid_regex("["))
        self.assertFalse(matches_regex("anything", "["))
        self.assertTrue(is_valid_regex(r"^asset_[0-9]+$"))
