# QuranSearch

This project retrieves Quranic ayahs from the AlQuran Cloud API, indexes them in an Elasticsearch database, and provides a searchable index of Quranic verses. The project utilizes Python, the requests library for API interaction, Elasticsearch for data storage, and Elasticsearch DSL for defining index mappings.

## Features

- Retrieves Quranic ayahs from AlQuran Cloud API for both Uthmani and Simple text versions.
- Indexes ayahs in an Elasticsearch database with proper mappings for effective searching.
- Provides an example of handling API rate limits and best practices for data retrieval.
- Supports customization of Elasticsearch credentials and connection settings.

## Requirements

- Python 3.x
- Elasticsearch 7.x (Make sure Elasticsearch is running on localhost:9200)

## Installation

1. Clone this repository: `git clone https://github.com/malsioufi/QuranSearch.git`
2. Navigate to the project directory: `cd QuranSearch`
3. Install required Python packages: `pip install -r requirements.txt`

## Configuration

1. Open the `main.py` file and replace `'your_username'` and `'your_password'` with your Elasticsearch authentication credentials.

## Usage

Run the script to retrieve and index Quranic ayahs:

```bash
python main.py
```

## Contributing
Contributions to this project are welcome! If you find any issues or have suggestions for improvements, feel free to create an issue or submit a pull request.

## Credits
- Data provided by AlQuran Cloud API ([http://api.alquran.cloud](http://api.alquran.cloud))
- Code developed with assistance from OpenAI's GPT-3 assistant and in collaboration with [Mohamad Alsioufi](https://github.com/malsioufi).

## License

This project is licensed under the [MIT License](LICENSE) - see the [LICENSE](LICENSE) file for details.
