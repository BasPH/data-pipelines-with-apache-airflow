#!/usr/bin/env python

import setuptools

requirements = ["apache-airflow", "requests"]

extra_requirements = {"dev": ["pytest"]}

setuptools.setup(
    name="airflow_movielens",
    version="0.1.0",
    description="Hooks, sensors and operators for the Movielens API.",
    author="Anonymous",
    author_email="anonymous@example.com",
    install_requires=requirements,
    extras_require=extra_requirements,
    packages=setuptools.find_packages("src"),
    package_dir={"": "src"},
    url="https://github.com/example-repo/airflow_movielens",
    license="MIT license",
)
