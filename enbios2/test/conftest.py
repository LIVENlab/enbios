import pytest

# from enbios2.test.test_bw2i import clean_project


def pytest_configure(config):
    config.option.capture = "no"


@pytest.hookimpl(tryfirst=True)
def pytest_sessionfinish(session, exitstatus):
    print("Cleaning up test data...")
    # would delte the project and cause data to be loaded again
    # clean_project()


