from pydantic import BaseModel


class ETLModel(BaseModel):
    """Base model for ETL data transfer objects.

    Extend this class to define schemas for your extracted/transformed data.

    Example:
        class UserRecord(ETLModel):
            id: int
            name: str
            email: str
    """
    pass
