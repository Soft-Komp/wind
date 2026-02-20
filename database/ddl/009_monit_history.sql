USE [WAPRO];
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'dbo_ext' AND t.name = 'MonitHistory'
)
BEGIN
    CREATE TABLE dbo_ext.MonitHistory (
        ID_MONIT        BIGINT          NOT NULL IDENTITY(1,1),
        ID_KONTRAHENTA  INT             NOT NULL,
        ID_USER         INT             NULL,
        MonitType       NVARCHAR(20)    NOT NULL,
        TemplateID      INT             NULL,
        Status          NVARCHAR(20)    NOT NULL DEFAULT 'pending',
        Recipient       NVARCHAR(100)   NULL,
        Subject         NVARCHAR(200)   NULL,
        MessageBody     NVARCHAR(MAX)   NULL,
        TotalDebt       DECIMAL(18,2)   NULL,
        InvoiceNumbers  NVARCHAR(500)   NULL,
        PDFPath         NVARCHAR(500)   NULL,
        ExternalID      NVARCHAR(100)   NULL,
        ScheduledAt     DATETIME        NULL,
        SentAt          DATETIME        NULL,
        DeliveredAt     DATETIME        NULL,
        OpenedAt        DATETIME        NULL,
        ClickedAt       DATETIME        NULL,
        ErrorMessage    NVARCHAR(500)   NULL,
        RetryCount      INT             NOT NULL DEFAULT 0,
        Cost            DECIMAL(10,4)   NULL,
        IsActive        BIT             NOT NULL DEFAULT 1,
        CreatedAt       DATETIME        NOT NULL DEFAULT GETDATE(),
        UpdatedAt       DATETIME        NULL,

        CONSTRAINT PK_MonitHistory PRIMARY KEY CLUSTERED (ID_MONIT),
        CONSTRAINT FK_MonitHistory_User
            FOREIGN KEY (ID_USER)
            REFERENCES dbo_ext.Users(ID_USER)
            ON DELETE SET NULL,
        CONSTRAINT FK_MonitHistory_Template
            FOREIGN KEY (TemplateID)
            REFERENCES dbo_ext.Templates(ID_TEMPLATE)
            ON DELETE SET NULL,
        CONSTRAINT CK_MonitHistory_Type
            CHECK (MonitType IN ('email', 'sms', 'print')),
        CONSTRAINT CK_MonitHistory_Status
            CHECK (Status IN (
                'pending','sent','delivered',
                'bounced','failed','opened','clicked'
            )),
        CONSTRAINT CK_MonitHistory_RetryCount
            CHECK (RetryCount >= 0)
    );
    PRINT 'Tabela dbo_ext.MonitHistory utworzona.';
END
GO

CREATE INDEX IX_MonitHist_Kontrahent ON dbo_ext.MonitHistory (ID_KONTRAHENTA);
CREATE INDEX IX_MonitHist_User       ON dbo_ext.MonitHistory (ID_USER);
CREATE INDEX IX_MonitHist_Type       ON dbo_ext.MonitHistory (MonitType);
CREATE INDEX IX_MonitHist_Status     ON dbo_ext.MonitHistory (Status);
CREATE INDEX IX_MonitHist_SentAt     ON dbo_ext.MonitHistory (SentAt DESC);
CREATE INDEX IX_MonitHist_CreatedAt  ON dbo_ext.MonitHistory (CreatedAt DESC);
GO