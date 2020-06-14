import inspect
from unittest import mock

import pytest

from aiomigrate import driver


@pytest.mark.parametrize('cls', [
    driver.Connection,
    driver.Pool,
    driver.Driver,
])
def test_interface(cls):
    """Ensure that interface class and its methods are abstract."""
    assert inspect.isabstract(cls), "Class '{}' is not abstract".format(cls.__name__)
    for method_name, method in inspect.getmembers(cls, predicate=inspect.isfunction):
        method_is_abstract = (
            hasattr(method, '__isabstractmethod__') and
            method.__isabstractmethod__
        )
        error_msg = "Method '{}' of class '{}' is not abstract".format(method_name, cls.__name__)
        assert method_is_abstract, error_msg


def test_driver_is_abstract():
    """Ensure that `driver.Driver` class is abstract."""
    assert inspect.isabstract(driver.Driver)


def fake_postgresql_entry_points(driver_cls):
    """Generate return value of `pkg_resources.iter_entry_points` for the postgresql driver."""
    class FakeEntryPoint:
        name = 'postgresql'
        dist = 'my-distribution'
        module_name = 'my.module.name'
        attrs = ('MyDriverClass',)
        resolve = mock.Mock(return_value=driver_cls)

    return iter([FakeEntryPoint()])


def test_get_driver_unsatisfied_interface(mocker):
    """Ensure that exception is raised when driver plugin doesn't implement Driver interface."""
    class BadDriver:
        ...

    expected_message = (
        "DriverExtensionMetadata(name='postgresql', distribution='my-distribution', "
        "object_location='my.module.name:MyDriverClass') found for database dsn "
        "'postgresql://' is not a subclass of Driver interface"
    )

    mocker.patch(
        'pkg_resources.iter_entry_points',
        return_value=fake_postgresql_entry_points(driver_cls=BadDriver),
    )
    with pytest.raises(RuntimeError) as excinfo:
        driver.get_driver('postgresql://postgres@localhost/test')
    assert str(excinfo.value) == expected_message


def test_get_driver_no_drivers_available(mocker):
    """Ensure that exception is raised when no available drivers found."""
    expected_message = (
        "No driver found for database dsn 'postgresql://', no available drivers found at all"
    )

    mocker.patch(
        'pkg_resources.iter_entry_points',
        return_value=iter([]),
    )
    with pytest.raises(RuntimeError) as excinfo:
        driver.get_driver('postgresql://postgres@localhost/test')
    assert str(excinfo.value) == expected_message


def test_get_driver_drivers_available_but_none_match(mocker):
    """Ensure that exception is raised when drivers available but none of them match DSN."""
    expected_message = (
        "No driver found for database dsn 'mysql://', "
        "available drivers are DriverExtensionMetadata(name='postgresql', "
        "distribution='my-distribution', object_location='my.module.name:MyDriverClass')"
    )

    class MyDriverClass(driver.Driver):
        def create_pool(self, dsn):
            ...

    mocker.patch(
        'pkg_resources.iter_entry_points',
        return_value=fake_postgresql_entry_points(MyDriverClass),
    )
    with pytest.raises(RuntimeError) as excinfo:
        driver.get_driver('mysql://user:password@host:port/db')
    assert str(excinfo.value) == expected_message


def test_get_driver_match_and_satisfied_interface(mocker):
    """Ensure that driver that match DSN and satisfy Driver interface could be loaded."""
    class MyDriverClass(driver.Driver):
        def create_pool(self, dsn):
            ...

    mocker.patch(
        'pkg_resources.iter_entry_points',
        return_value=fake_postgresql_entry_points(MyDriverClass),
    )
    driver_instance = driver.get_driver('postgresql://postgres@localhost/test')
    assert isinstance(driver_instance, MyDriverClass)
