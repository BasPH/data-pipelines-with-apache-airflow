import setuptools

setuptools.setup(
    name="airflowbook",
    version="0.1",
    packages=setuptools.find_packages(where="src"),
    package_dir={"": "src"},
    # install_requires=[...],
    extras_require={"dev": ["pytest_docker_tools~=0.2.0"]},
)
