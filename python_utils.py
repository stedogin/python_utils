from os import environ


def get_environment_variable(variable_name):
    if variable_name in environ:
        return environ[variable_name]
    else:
        raise EnvironmentError(f"Missing environment variable: {variable_name}")
