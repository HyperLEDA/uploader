# HyperLEDA Uploader

A command-line tool for uploading data to HyperLEDA.

## Installation

You can install the tool using pip:

```bash
pip install .
```

Or install it directly from the repository:

```bash
pip install git+https://github.com/yourusername/hyperleda-uploader.git
```

## Usage

After installation, you can use the tool via the `hyperleda-upload` command:

```bash
# Discover available plugins
hyperleda-upload discover

# Upload data using a plugin
hyperleda-upload upload <plugin-name> [options]

# Get help
hyperleda-upload --help
```

### Environment Selection

The tool supports different environments (dev, test, prod) which can be selected using the `--endpoint` option:

```bash
hyperleda-upload --endpoint prod upload <plugin-name>
```

### Plugin Development

Plugins should be placed in the `plugins` directory (or a custom directory specified with `--plugin-dir`). Each plugin should implement the `UploaderPlugin` interface.

## Development

To set up the development environment:

1. Clone the repository
2. Install development dependencies:
   ```bash
   pip install -e ".[dev]"
   ```
3. Run tests:
   ```bash
   pytest
   ```