#!/usr/bin/env python

import setuptools

requirements = ["apache-airflow", "requests"]

extra_requirements = {"dev": ["pytest"]}

setuptools.setup(
    name="airflow_movielens",
    version="0.1.0",
    author="John Smith",
    author_email="john.smith@example.com",
    description="Hooks, sensors and operators for the Movielens API.",
    install_requires=requirements,
    extras_require=extra_requirements,
    packages=setuptools.find_packages("src"),
    package_dir={"": "src"},
    url="https://github.com/example-repo/airflow_movielens",
    license="MIT license",
)
