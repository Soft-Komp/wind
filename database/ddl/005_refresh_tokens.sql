-- ============================================================
-- database/ddl/005_refresh_tokens.sql
-- Tabela: dbo_ext.skw_RefreshTokens
--
-- Tokeny odświeżania JWT (HttpOnly cookie).
-- Token przechowywany jako SHA-256 hash — NIGDY plain.
-- Brak UpdatedAt i IsActive — tokeny są immutable:
--   revoke = ustawienie IsRevoked = 1 + RevokedAt = GETDATE().
--
-- Czyszczenie: ARQ cron lub periodic task (wygasłe + odwołane).
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
          AND  t.name = 'skw_RefreshTokens'
    )
    BEGIN
        PRINT '[005] Tworzenie tabeli dbo_ext.skw_RefreshTokens...';

        CREATE TABLE [dbo_ext].[skw_RefreshTokens] (

            -- ── Klucz główny ──────────────────────────────────────────────────
            [ID_TOKEN]    INT            IDENTITY(1,1)  NOT NULL,

            -- ── Właściciel tokenu ────────────────────────────────────────────
            [ID_USER]     INT                           NOT NULL,

            -- ── Token (SHA-256 hash, nie plain JWT) ──────────────────────────
            [Token]       NVARCHAR(500)                 NOT NULL,

            -- ── Ważność ───────────────────────────────────────────────────────
            [ExpiresAt]   DATETIME                      NOT NULL,

            -- ── Odwołanie ────────────────────────────────────────────────────
            [IsRevoked]   BIT                           NOT NULL
                          CONSTRAINT [DF_skw_RefreshTokens_IsRevoked] DEFAULT (0),
            [RevokedAt]   DATETIME                          NULL,

            -- ── Metadane sesji ───────────────────────────────────────────────
            [IPAddress]   NVARCHAR(45)                      NULL,   -- IPv4 lub IPv6
            [UserAgent]   NVARCHAR(500)                     NULL,

            -- ── Timestamp (brak UpdatedAt — immutable) ────────────────────────
            [CreatedAt]   DATETIME                      NOT NULL
                          CONSTRAINT [DF_skw_RefreshTokens_CreatedAt] DEFAULT (GETDATE()),

            -- ── Constraints ──────────────────────────────────────────────────
            CONSTRAINT [PK_skw_RefreshTokens]
                PRIMARY KEY CLUSTERED ([ID_TOKEN] ASC),

            CONSTRAINT [UQ_skw_RefreshTokens_Token]
                UNIQUE ([Token]),

            -- FK → skw_Users (usunięcie usera = usunięcie jego tokenów)
            CONSTRAINT [FK_skw_RefreshTokens_UserID]
                FOREIGN KEY ([ID_USER])
                REFERENCES [dbo_ext].[skw_Users] ([ID_USER])
                ON DELETE CASCADE
                ON UPDATE NO ACTION
        );

        PRINT '[005] Tabela dbo_ext.skw_RefreshTokens utworzona.';
    END
    ELSE
    BEGIN
        PRINT '[005] Tabela dbo_ext.skw_RefreshTokens już istnieje — pominięto.';
    END

    -- ── Indeksy ───────────────────────────────────────────────────────────────

    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE  object_id = OBJECT_ID('dbo_ext.skw_RefreshTokens')
          AND  name      = 'IX_skw_RefreshTokens_UserID'
    )
    BEGIN
        CREATE NONCLUSTERED INDEX [IX_skw_RefreshTokens_UserID]
            ON [dbo_ext].[skw_RefreshTokens] ([ID_USER] ASC, [IsRevoked] ASC);
        PRINT '[005] Indeks IX_skw_RefreshTokens_UserID utworzony.';
    END

    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE  object_id = OBJECT_ID('dbo_ext.skw_RefreshTokens')
          AND  name      = 'IX_skw_RefreshTokens_ExpiresAt'
    )
    BEGIN
        -- Używany przez ARQ cleanup task
        CREATE NONCLUSTERED INDEX [IX_skw_RefreshTokens_ExpiresAt]
            ON [dbo_ext].[skw_RefreshTokens] ([ExpiresAt] ASC)
            WHERE [IsRevoked] = 0;
        PRINT '[005] Indeks IX_skw_RefreshTokens_ExpiresAt utworzony.';
    END

    COMMIT TRANSACTION;
    PRINT '[005] === DDL 005: skw_RefreshTokens — OK ===';

END TRY
BEGIN CATCH
    ROLLBACK TRANSACTION;
    PRINT '[005] BŁĄD: ' + ERROR_MESSAGE();
    THROW;
END CATCH
GO