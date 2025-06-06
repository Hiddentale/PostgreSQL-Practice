from setuptools import setup, find_packages

setup(
    name="postgresql-practice",
    version="0.1.0",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    install_requires=[
        "psycopg2-binary>=2.9.0",
    ],
    python_requires=">=3.6",
)