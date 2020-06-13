from aiomigrate import run


def test_main_logging(mocker):
    dict_config = mocker.patch('logging.config.dictConfig')
    log_level = 'INFO'
    run.main()
    assert dict_config.called_with({
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'simple': {
                'format': '%(asctime)s [%(levelname)s] (%(name)s) %(message)s',
                'datefmt': '%Y-%m-%dT%H:%M:%S%Z',
            },
        },
        'handlers': {
            'stdout': {
                'class': 'logging.StreamHandler',
                'stream': 'ext://sys.stdout',
                'level': log_level,
                'formatter': 'simple',
            },
        },
        'loggers': {
            '': {
                'handlers': ['stdout'],
                'level': log_level,
            },
        },
    })
