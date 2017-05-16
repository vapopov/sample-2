import apps
import imp
import importlib
import pkgutil
from warnings import warn


__all__ = ('autodiscover', 'find_related_module')
_RACE_PROTECTION = False


def autodiscover(related_name='__init__'):
    """ return all modules  """

    global _RACE_PROTECTION

    if _RACE_PROTECTION:
        return ()
    _RACE_PROTECTION = True
    try:
        applications = ['apps.' + name for _, name, is_package in pkgutil.iter_modules(apps.__path__) if is_package]
        return [_f for _f in [find_related_module(app, related_name=related_name) for app in applications] if _f]
    finally:
        _RACE_PROTECTION = False


def find_related_module(app, related_name):
    """
    Given a package name and a module name, tries to find that module.
    """
    try:
        app_path = importlib.import_module(app).__path__
    except ImportError as exc:
        warn('Autodiscover: Error importing %s.%s: %r' % (
            app, related_name, exc,
        ))
        return
    except AttributeError:
        return
    try:
        imp.find_module(related_name, app_path)
    except ImportError:
        return
    return importlib.import_module('{0}.{1}'.format(app, related_name))
