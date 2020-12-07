from pathlib import Path

# Filepath directory for saved DBs of scraped conferences and journals

file_name = 'service_marq'
curr_dir = Path(__file__).parent.resolve()

CRAWL_FILEPATH = Path.joinpath(curr_dir, '{}/'.format(file_name))
DB_FILEPATH = Path.joinpath(CRAWL_FILEPATH, "{}.db".format(file_name))
LOG_FILEPATH = Path.joinpath(CRAWL_FILEPATH, '{}.log'.format(file_name))

CRAWL_FILEPATH.mkdir(parents=True, exist_ok=True)