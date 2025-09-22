#!/usr/bin/python3

import os
from datetime import datetime

class Logger:
    COLORS = {
        "ENDC": "\033[0m",
        "HEADER": "\033[95m",
        "OKBLUE": "\033[94m",
        "OKCYAN": "\033[96m",
        "OKGREEN": "\033[92m",
        "WARNING": "\033[93m",
        "FAIL": "\033[91m",
        "BOLD": "\033[1m",
        "UNDERLINE": "\033[4m"
    }

    def __init__(self, log_dir: str = "logs", log_prefix: str = "db_worker"):
        """
        Initialize logger and prepare log file path.
        """
        os.makedirs(log_dir, exist_ok=True)
        log_filename = f"{log_prefix}_{datetime.now().strftime('%Y%m%d')}.log"
        self.log_path = os.path.join(log_dir, log_filename)
        self.fhandle = None
        self.show_time = True

    def _init_filehandle(self) -> None:
        """
        Open file handle for log writing if not already opened.
        """
        if not self.fhandle:
            self.fhandle = open(self.log_path, "a", encoding="UTF-8")

    def _colorize(self, text: str, color: str) -> str:
        """
        Wrap text with ANSI color codes.
        """
        return f"{self.COLORS.get(color, '')}{text}{self.COLORS['ENDC']}"

    def _timestamp(self) -> str:
        """
        Return formatted timestamp string.
        """
        if self.show_time:
            ndatetime = datetime.now().astimezone()
            return ndatetime.strftime("%Y-%m-%d %H:%M:%S.") + f"{ndatetime.microsecond // 1000:03d}"
        return ""

    def _to_log(self, console_output: str, log_output: str) -> None:
        """
        Output to console and write to log file (no color).
        """
        print(console_output)
        self._init_filehandle()
        self.fhandle.write(log_output + "\n")
        self.fhandle.flush()

    def log_info(self, message: str) -> None:
        """
        Print and log info-level message.
        """
        tstamp = self._timestamp()
        color_start = self.COLORS.get("OKBLUE", "")
        color_end = self.COLORS["ENDC"]
        log_output = f"[{tstamp}] [INFO] {message}"
        console_output = f"[{tstamp}] {color_start}[INFO]{color_end} {message}"
        self._to_log(console_output, log_output)

    def log_warn(self, message: str) -> None:
        """
        Print and log warning-level message.
        """
        tstamp = self._timestamp()
        color_start = self.COLORS.get("WARNING", "")
        color_end = self.COLORS["ENDC"]
        log_output = f"[{tstamp}] [WARN] {message}"
        console_output = f"[{tstamp}] {color_start}[WARN]{color_end} {message}"
        self._to_log(console_output, log_output)

    def log_error(self, message: str) -> None:
        """
        Print and log error-level message.
        """
        tstamp = self._timestamp()
        color_start = self.COLORS.get("FAIL", "")
        color_end = self.COLORS["ENDC"]
        log_output = f"[{tstamp}] [ERR]  {message}"
        console_output = f"[{tstamp}] {color_start}[ERR]{color_end}  {message}"
        self._to_log(console_output, log_output)

    def log_ok(self, message: str) -> None:
        """
        Print and log success/ok-level message.
        """
        tstamp = self._timestamp()
        color_start = self.COLORS.get("OKGREEN", "")
        color_end = self.COLORS["ENDC"]
        log_output = f"[{tstamp}] [ OK ] {message}"
        console_output = f"[{tstamp}] {color_start}[ OK ]{color_end} {message}"
        self._to_log(console_output, log_output)

    def log_title(self, message: str) -> None:
        """
        Print a title section with borders, and log the same block without color codes.
        Dynamically adjusts width based on message length, with a minimum width of 75.
        """
        tstamp = self._timestamp()
        color_start = f"{self.COLORS.get('BOLD', '')}{self.COLORS.get('HEADER', '')}"
        color_end = f"{self.COLORS['ENDC']}{self.COLORS['ENDC']}"

        # Calculate required width based on message length, with minimum of 75
        base_content = f"# [{tstamp}] {message} #"
        width = max(75, len(base_content) + 4)  # Add padding for better appearance
        
        head_line = "#" + "#" * (width - 2) + "#\n"
        middle_line = "#" + " " * (width - 2) + "#\n"

        # Center the message in the available space
        content_without_borders = f"[{tstamp}] {message}"
        padding = width - 4 - len(content_without_borders)  # 4 for '# ' and ' #'
        left_padding = padding // 2
        right_padding = padding - left_padding
        title_line = f"# {' ' * left_padding}{content_without_borders}{' ' * right_padding} #\n"

        console_output = "\n" + color_start + head_line + middle_line + title_line + middle_line + head_line + color_end
        log_output = "\n" + head_line + middle_line + title_line + middle_line + head_line

        self._to_log(console_output, log_output)

    def log_sub_title(self, message: str) -> None:
        """
        Print a title section with borders, and log the same block without color codes.
        Dynamically adjusts width based on message length, with a minimum width of 75.
        """
        tstamp = self._timestamp()
        color_start = f"{self.COLORS.get('BOLD', '')}{self.COLORS.get('OKGREEN', '')}"
        color_end = f"{self.COLORS['ENDC']}{self.COLORS['ENDC']}"

        # Calculate required width based on message length, with minimum of 75
        base_content = f"# [{tstamp}] {message} #"
        width = max(75, len(base_content) + 4)  # Add padding for better appearance
        
        head_line = "#" + "#" * (width - 2) + "#\n"

        # Center the message in the available space
        content_without_borders = f"[{tstamp}] {message}"
        padding = width - 4 - len(content_without_borders)  # 4 for '# ' and ' #'
        left_padding = padding // 2
        right_padding = padding - left_padding
        title_line = f"# {' ' * left_padding}{content_without_borders}{' ' * right_padding} #\n"

        console_output = "\n" + color_start + head_line + title_line + head_line + color_end
        log_output = "\n" + head_line + title_line + head_line

        self._to_log(console_output, log_output)

    def log_insert_exception(self, filename: str, sqlcmd: str, values, exception: Exception) -> None:
        """
        Log details of a SQL insert exception.
        """
        self.log_error(f"SQL insert error, filename: {filename}")
        self.log_info(f"SQL: {sqlcmd}")
        self.log_info(f"Values: {values}")
        self.log_info(f"Exception: {exception}")

    def log_parse_fail(self, filename: str, reason: str = "Unknown reason") -> None:
        """
        Log failure in parsing a file.
        """
        self.log_error(f"Parse failed, filename: {filename}, reason: {reason}")


if __name__ == "__main__":
    logger = Logger()
    logger.log_title("This is title")
    logger.log_info("This is information")
    logger.log_warn("This is warning")
    logger.log_error("This is error")
