#!/usr/bin/env python3
"""
File: db/init.py

Purpose:
    Initialize the Microsoft SQL Server schema required by the BitSight SDK
    and command-line interface.

Responsibilities:
    - Validate the configured schema file path.
    - Establish the target MSSQL database connection.
    - Load SQL schema definitions using UTF-8 encoding.
    - Normalize byte-order marks and platform-specific line endings.
    - Split the schema file into executable SQL statements.
    - Execute schema statements in deterministic source order.
    - Commit the schema transaction only after all statements succeed.
    - Roll back the transaction when any statement fails.
    - Close the database connection regardless of execution outcome.
    - Emit operationally useful logs without exposing database credentials.

Schema contract:
    - Executable SQL statements must be terminated with semicolons.
    - Statement ordering in the schema file is preserved.
    - Single-line and block comments are preserved with their associated SQL.
    - A final non-empty statement may omit a terminating semicolon.
    - SQL Server GO batch separators are not processed by this initializer.
    - The schema file must be readable as UTF-8 text.

Transaction contract:
    - All schema statements execute through one MSSQLDatabase instance.
    - The transaction is committed only after every statement succeeds.
    - Any execution failure triggers an immediate rollback.
    - The original exception is re-raised after rollback.
    - The database connection is closed exactly once by run().

Security:
    - Database passwords must never be logged.
    - SQL previews are truncated before logging.
    - Schema files should be trusted deployment artifacts.
    - This initializer executes the schema file with the privileges of the
      configured database account.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import List

from db.mssql import MSSQLDatabase


logger = logging.getLogger(__name__)


class MSSQLInitializer:
    """
    Initialize the BitSight schema in a Microsoft SQL Server database.

    One initializer instance owns one database connection and one schema file.

    Lifecycle:
        1. Validate constructor arguments.
        2. Resolve and validate the schema path.
        3. Create the MSSQL database implementation.
        4. Load and split the schema file.
        5. Execute statements in source order.
        6. Commit on success or roll back on failure.
        7. Close the database connection.

    Instances are single-use. After run() completes, the underlying database
    connection has been closed.
    """

    def __init__(
        self,
        server: str,
        database: str,
        username: str,
        password: str,
        schema_path: str,
    ) -> None:
        """
        Initialize the MSSQL schema executor.

        Args:
            server:
                SQL Server hostname, address, or configured server identifier.

            database:
                Target database name.

            username:
                Database login username.

            password:
                Database login password.

            schema_path:
                Path to the UTF-8 SQL schema file.

        Raises:
            TypeError:
                If any argument is not a string.

            ValueError:
                If a required connection value or schema path is empty.

            FileNotFoundError:
                If the schema file does not exist.

            IsADirectoryError:
                If schema_path identifies a directory instead of a file.

            PermissionError:
                If the schema file is not readable.

            Exception:
                If MSSQLDatabase cannot initialize its connection.
        """
        self._validate_non_empty_string(server, "server")
        self._validate_non_empty_string(database, "database")
        self._validate_non_empty_string(username, "username")

        if not isinstance(password, str):
            raise TypeError("password must be a string")

        self._validate_non_empty_string(schema_path, "schema_path")

        self.schema_path = Path(schema_path).expanduser()

        if not self.schema_path.exists():
            raise FileNotFoundError(
                f"Schema file not found: {self.schema_path}"
            )

        if not self.schema_path.is_file():
            raise IsADirectoryError(
                f"Schema path is not a file: {self.schema_path}"
            )

        self.db = MSSQLDatabase(
            server=server.strip(),
            database=database.strip(),
            username=username.strip(),
            password=password,
        )

    # -----------------------------------------------------------------------
    # Public execution
    # -----------------------------------------------------------------------

    def run(self) -> None:
        """
        Apply the configured MSSQL schema transactionally.

        The schema file is loaded before statement execution begins. Statements
        are executed sequentially through the configured MSSQLDatabase object.

        Returns:
            None.

        Raises:
            OSError:
                If the schema file cannot be read.

            UnicodeDecodeError:
                If the schema file is not valid UTF-8.

            ValueError:
                If the schema file contains no executable statements.

            Exception:
                If statement execution, commit, rollback, or connection
                shutdown fails.

        Transaction behavior:
            - Commit occurs only after every statement succeeds.
            - Any statement or commit failure triggers rollback.
            - The database connection is always closed.
        """
        logger.info(
            "Initializing BitSight MSSQL schema | file=%s",
            self.schema_path,
        )

        statements = self._load_schema_statements()

        if not statements:
            raise ValueError(
                f"Schema file contains no executable statements: "
                f"{self.schema_path}"
            )

        total_statements = len(statements)

        try:
            for statement_number, statement in enumerate(
                statements,
                start=1,
            ):
                preview = self._build_statement_preview(statement)

                logger.info(
                    "Applying MSSQL schema statement | statement=%d/%d "
                    "preview=%s",
                    statement_number,
                    total_statements,
                    preview,
                )

                self.db.execute(statement)

            self.db.commit()

            logger.info(
                "MSSQL schema initialized successfully | statements=%d",
                total_statements,
            )

        except Exception:
            logger.exception(
                "MSSQL schema initialization failed; rolling back transaction"
            )

            try:
                self.db.rollback()
            except Exception:
                logger.exception(
                    "MSSQL schema rollback failed"
                )

            raise

        finally:
            try:
                self.db.close()
            except Exception:
                logger.exception(
                    "Failed to close MSSQL database connection"
                )
                raise

    # -----------------------------------------------------------------------
    # Schema loading and parsing
    # -----------------------------------------------------------------------

    def _load_schema_statements(self) -> List[str]:
        """
        Load and split the schema file into executable MSSQL statements.

        Statement boundary rules:
            - A statement ends when a semicolon appears outside a quoted string,
              bracketed identifier, line comment, or block comment.
            - Semicolons inside SQL string literals are preserved.
            - Semicolons inside comments are preserved.
            - Escaped single quotes represented by two consecutive apostrophes
              are handled correctly.
            - A final non-empty statement is accepted without a terminating
              semicolon.
            - Trailing statement terminators are removed before execution.

        Returns:
            SQL statements in their original source order.

        Raises:
            OSError:
                If the file cannot be read.

            UnicodeDecodeError:
                If the file is not valid UTF-8.

            ValueError:
                If the SQL contains an unterminated string literal, bracketed
                identifier, or block comment.
        """
        raw_sql = self.schema_path.read_text(encoding="utf-8")

        # Remove a UTF-8 byte-order mark and normalize all line endings so
        # parsing and logging behave consistently across operating systems.
        normalized_sql = (
            raw_sql
            .replace("\ufeff", "")
            .replace("\r\n", "\n")
            .replace("\r", "\n")
        )

        statements: List[str] = []
        buffer: List[str] = []

        in_single_quote = False
        in_bracket_identifier = False
        in_line_comment = False
        in_block_comment = False

        index = 0
        sql_length = len(normalized_sql)

        while index < sql_length:
            character = normalized_sql[index]
            next_character = (
                normalized_sql[index + 1]
                if index + 1 < sql_length
                else ""
            )

            # Line comments terminate at the newline character.
            if in_line_comment:
                buffer.append(character)

                if character == "\n":
                    in_line_comment = False

                index += 1
                continue

            # Block comments terminate only when the closing delimiter appears.
            if in_block_comment:
                buffer.append(character)

                if character == "*" and next_character == "/":
                    buffer.append(next_character)
                    in_block_comment = False
                    index += 2
                    continue

                index += 1
                continue

            # SQL Server bracketed identifiers may contain characters that
            # would otherwise be interpreted as parser delimiters.
            if in_bracket_identifier:
                buffer.append(character)

                if character == "]":
                    if next_character == "]":
                        buffer.append(next_character)
                        index += 2
                        continue

                    in_bracket_identifier = False

                index += 1
                continue

            # SQL string literals use doubled apostrophes to represent an
            # embedded apostrophe.
            if in_single_quote:
                buffer.append(character)

                if character == "'":
                    if next_character == "'":
                        buffer.append(next_character)
                        index += 2
                        continue

                    in_single_quote = False

                index += 1
                continue

            if character == "-" and next_character == "-":
                buffer.extend((character, next_character))
                in_line_comment = True
                index += 2
                continue

            if character == "/" and next_character == "*":
                buffer.extend((character, next_character))
                in_block_comment = True
                index += 2
                continue

            if character == "'":
                buffer.append(character)
                in_single_quote = True
                index += 1
                continue

            if character == "[":
                buffer.append(character)
                in_bracket_identifier = True
                index += 1
                continue

            if character == ";":
                statement = "".join(buffer).strip()

                if statement:
                    statements.append(statement)

                buffer.clear()
                index += 1
                continue

            buffer.append(character)
            index += 1

        if in_single_quote:
            raise ValueError(
                "Schema file contains an unterminated SQL string literal"
            )

        if in_bracket_identifier:
            raise ValueError(
                "Schema file contains an unterminated bracketed identifier"
            )

        if in_block_comment:
            raise ValueError(
                "Schema file contains an unterminated block comment"
            )

        trailing_statement = "".join(buffer).strip()

        if trailing_statement:
            statements.append(trailing_statement)

        logger.info(
            "Loaded MSSQL schema statements | count=%d",
            len(statements),
        )

        return statements

    # -----------------------------------------------------------------------
    # Internal validation and logging helpers
    # -----------------------------------------------------------------------

    @staticmethod
    def _validate_non_empty_string(value: str, field_name: str) -> None:
        """
        Validate a required string argument.

        Args:
            value:
                Value to validate.

            field_name:
                Argument name used in exception messages.

        Raises:
            TypeError:
                If value is not a string.

            ValueError:
                If value is empty or contains only whitespace.
        """
        if not isinstance(value, str):
            raise TypeError(f"{field_name} must be a string")

        if not value.strip():
            raise ValueError(f"{field_name} must not be empty")

    @staticmethod
    def _build_statement_preview(
        statement: str,
        maximum_length: int = 180,
    ) -> str:
        """
        Build a single-line SQL preview for operational logging.

        Args:
            statement:
                SQL statement to summarize.

            maximum_length:
                Maximum preview length.

        Returns:
            Whitespace-normalized and truncated statement preview.
        """
        normalized = " ".join(statement.split())

        if len(normalized) <= maximum_length:
            return normalized

        return f"{normalized[:maximum_length - 3]}..."
