CREATE TABLE dbo.bitsight_companies (
    company_guid            UNIQUEIDENTIFIER NOT NULL,
    name                    NVARCHAR(255) NOT NULL,
    domain                  NVARCHAR(255) NULL,
    industry                NVARCHAR(255) NULL,
    sub_industry             NVARCHAR(255) NULL,
    country                 NVARCHAR(64) NULL,
    added_date              DATETIME2 NULL,
    rating                  INT NULL,
    ingested_at             DATETIME2 NOT NULL,
    raw_payload              NVARCHAR(MAX) NOT NULL,

    CONSTRAINT PK_bitsight_companies
        PRIMARY KEY (company_guid)
);
