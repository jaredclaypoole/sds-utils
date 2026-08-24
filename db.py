from sqlmodel import SQLModel, Field, create_engine


class AssetPartitionHistory(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    asset: str
    partition: str
    stale_success: bool


db_url = "sqlite:///db/app.db"
engine = create_engine(db_url, echo=False)


def create_db_and_tables():
    SQLModel.metadata.create_all(engine)
