-- ============================================================
-- database/ddl/006_otp_codes.sql
-- Tabela: dbo_ext.skw_OtpCodes
--
-- Jednorazowe kody OTP (reset hasła, 2FA).
-- Kod przechowywany jako hash bcrypt — NIGDY plain 6 cyfr.
-- Brak UpdatedAt — kody są jednorazowe i immutable.
-- TTL z SystemConfig: otp.expiry_minutes (domyślnie 15 min).
--
-- Powiązania:
--   → skw_Users.ID_USER (FK CASCADE DELETE)
-- ============================================================

USE [WAPRO];
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

BEGIN TRANSACTION;
BEGIN TRY

    IF NOT EXISTS (
        SELECT 1
        FROM   sys.tables  t
        JOIN   sys.schemas s ON t.schema_id = s.schema_id
        WHERE  s.name = 'dbo_ext'
          AND  t.name = 'skw_OtpCodes'
    )
    BEGIN
        PRINT '[006] Tworzenie tabeli dbo_ext.skw_OtpCodes...';

        CREATE TABLE [dbo_ext].[skw_OtpCodes] (

            -- ── Klucz główny ──────────────────────────────────────────────────
            [ID_OTP]    INT           IDENTITY(1,1)  NOT NULL,

            -- ── Właściciel ───────────────────────────────────────────────────
            [ID_USER]   INT                          NOT NULL,

            -- ── Kod (bcrypt hash — nie plain) ────────────────────────────────
            [Code]      NVARCHAR(10)                 NOT NULL,

            -- ── Cel użycia ───────────────────────────────────────────────────
            [Purpose]   NVARCHAR(20)                 NOT NULL,

            -- ── Ważność ───────────────────────────────────────────────────────
            [ExpiresAt] DATETIME                     NOT NULL,

            -- ── Status użycia (jednorazowy) ──────────────────────────────────
            [IsUsed]    BIT                          NOT NULL
                        CONSTRAINT [DF_skw_OtpCodes_IsUsed] DEFAULT (0),

            -- ── Metadane ────────────────────────────────────────────────────
            [IPAddress] NVARCHAR(45)                     NULL,

            -- ── Timestamp (brak UpdatedAt — immutable) ────────────────────────
            [CreatedAt] DATETIME                     NOT NULL
                        CONSTRAINT [DF_skw_OtpCodes_CreatedAt] DEFAULT (GETDATE()),

            -- ── Constraints ──────────────────────────────────────────────────
            CONSTRAINT [PK_skw_OtpCodes]
                PRIMARY KEY CLUSTERED ([ID_OTP] ASC),

            -- Tylko dozwolone typy OTP
            CONSTRAINT [CK_skw_OtpCodes_Purpose]
                CHECK ([Purpose] IN ('password_reset', '2fa')),

            -- FK → skw_Users (usunięcie usera = usunięcie jego kodów OTP)
            CONSTRAINT [FK_skw_OtpCodes_UserID]
                FOREIGN KEY ([ID_USER])
                REFERENCES [dbo_ext].[skw_Users] ([ID_USER])
                ON DELETE CASCADE
                ON UPDATE NO ACTION
        );

        PRINT '[006] Tabela dbo_ext.skw_OtpCodes utworzona.';
    END
    ELSE
    BEGIN
        PRINT '[006] Tabela dbo_ext.skw_OtpCodes już istnieje — pominięto.';
    END

    -- ── Indeksy ───────────────────────────────────────────────────────────────

    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE  object_id = OBJECT_ID('dbo_ext.skw_OtpCodes')
          AND  name      = 'IX_skw_OtpCodes_UserID_Purpose'
    )
    BEGIN
        CREATE NONCLUSTERED INDEX [IX_skw_OtpCodes_UserID_Purpose]
            ON [dbo_ext].[skw_OtpCodes] ([ID_USER] ASC, [Purpose] ASC, [IsUsed] ASC)
            WHERE [IsUsed] = 0;
        PRINT '[006] Indeks IX_skw_OtpCodes_UserID_Purpose utworzony.';
    END

    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE  object_id = OBJECT_ID('dbo_ext.skw_OtpCodes')
          AND  name      = 'IX_skw_OtpCodes_ExpiresAt'
    )
    BEGIN
        CREATE NONCLUSTERED INDEX [IX_skw_OtpCodes_ExpiresAt]
            ON [dbo_ext].[skw_OtpCodes] ([ExpiresAt] ASC)
            WHERE [IsUsed] = 0;
        PRINT '[006] Indeks IX_skw_OtpCodes_ExpiresAt utworzony.';
    END

    COMMIT TRANSACTION;
    PRINT '[006] === DDL 006: skw_OtpCodes — OK ===';

END TRY
BEGIN CATCH
    ROLLBACK TRANSACTION;
    PRINT '[006] BŁĄD: ' + ERROR_MESSAGE();
    THROW;
END CATCH
GO