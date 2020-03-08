import setuptools

setuptools.setup(
    name="airflowbook",
    version="0.1",
    packages=setuptools.find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.6.*",
)
