# data_dictionary_teradata
Create a data dictionary by having an AI process the first 5 rows of each column in a table

### Python dependencies

    pip install teradatasql pandas requests


# macOS

    brew install ollama

# Windows

    https://ollama.com/download

# Linux install

    curl -fsSL https://ollama.com/install.sh | sh

### add gpu support for linux

    curl -fsSL https://ollama.com/download/ollama-linux-amd64-rocm.tgz | sudo tar zx -C /usr

# ollama commands

## Loads the model into memory

    ollama run gemma3:4b

## Stops model removes from memory

    ollama stop gemma3:4b

## Creates server to access local model on port 11434

    ollama serve

    

