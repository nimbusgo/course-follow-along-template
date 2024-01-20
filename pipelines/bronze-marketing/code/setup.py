from setuptools import setup, find_packages
setup(
    name = 'demo_bronze-marketing',
    version = '1.0',
    packages = find_packages(include = ('bronzemarketing*', )) + ['prophecy_config_instances'],
    package_dir = {'prophecy_config_instances' : 'configs/resources/config'},
    package_data = {'prophecy_config_instances' : ['*.json', '*.py', '*.conf']},
    description = 'workflow',
    install_requires = [
'prophecy-libs==1.7.4'],
    entry_points = {
'console_scripts' : [
'main = bronzemarketing.pipeline:main'], },
    data_files = [(".prophecy", [".prophecy/workflow.latest.json"])],
    extras_require = {
'test' : ['pytest', 'pytest-html'], }
)
