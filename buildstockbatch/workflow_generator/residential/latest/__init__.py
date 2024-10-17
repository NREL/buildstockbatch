__version__ = "2024.10.09"
version_info = {
    "version": __version__,
    "version_description": """
Added "measures" block to upgrades. This is in-addition to options block. Now, either a list of measures,
or a list of options or both can be applied as a part of an upgrade. For measures, apply_logic and costs,
if they apply, should be handled within the measure. Only a dict of arguments can be passed to the measure.
Currently, the package apply logic only applies to options. Measures are always run if they are specified.
""",
}
