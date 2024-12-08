# OakTree

OakTree is a server infrastructure that handles database management, logging, and environment configurations to ensure seamless and efficient operations.

## Features

- **Database Management**: Simplifies connections and interactions with MySQL databases using the `ot_db_manager` module.
- **Custom Logging**: Provides robust logging capabilities with the `ot_logging` module, supporting both file and database logging.
- **Environment Configuration**: Manages multiple environments (Development, Staging, Production) with the `ot_environment` module.

## Installation

1. **Clone the Repository**
   ```bash
   git clone https://github.com/yourusername/oaktree.git
   cd oaktree
   ```

2. **Install Dependencies**
   Ensure you have Python installed. Then, run the installation script:
   ```bash
   ./install_NET.sh
   ```

## Usage

### Database Manager
```python
from ot_db_manager import ot_db_manager

# Initialize the database manager
db_manager = ot_db_manager(
    system='your_db_host',
    uid='your_username',
    pwd='your_password',
    library='your_library',
    table_name='your_table'
)

# Push a DataFrame to the database
import pandas as pd
df = pd.DataFrame({'column1': [1, 2], 'column2': ['a', 'b']})
db_manager.push_df_to_db(df)
```

### Logging
```python
from ot_logging import OtLogging

# Initialize the logger
logger = OtLogging(process_name='your_process').get_logger()

# Log messages
logger.info("This is an info message.")
logger.error("This is an error message.")
```

### Environment Configuration
```python
from ot_environment import cm_environment

# Get the current environment
env = cm_environment().get_environment()
print(env)
```

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any improvements or bug fixes.

## License

This project is licensed under the [MIT License](LICENSE).