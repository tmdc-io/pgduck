import logging
import logging.config
import os

log_formatter_file_path = os.path.join("logger/logging_config.ini")


def update_log_level(config_file_path):
    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    with open(config_file_path, 'r') as config_file:
        config_lines = config_file.readlines()

    updated_config_lines = []
    in_target_section = False

    for line in config_lines:
        if line.strip().startswith(f"[logger_") or line.strip().startswith(f"[handler_"):
            in_target_section = True
            updated_config_lines.append(line)
            continue

        if in_target_section and line.strip().startswith("level="):
            updated_config_lines.append(f"level={log_level}\n")
            in_target_section = False
            continue

        updated_config_lines.append(line)

    with open(config_file_path, 'w') as config_file:
        config_file.writelines(updated_config_lines)


def get_logger(name):
    # Updating log Level from env var
    update_log_level(log_formatter_file_path)
    # Setting log configs
    logging.config.fileConfig(fname=log_formatter_file_path, disable_existing_loggers=False)
    # Log level needs to be updated
    logger = logging.getLogger(name)
    return logger


log = get_logger("flash")
