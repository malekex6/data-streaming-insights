"""Setup file for Dataflow package."""

from setuptools import setup, find_packages

setup(
    name="insightstream-dataflow",
    version="1.0.0",
    description="InsightStream Dataflow streaming pipeline",
    author="Data Engineering Team",
    packages=find_packages(),
    install_requires=[
        "apache-beam[gcp]==2.51.0",
        "google-cloud-bigquery==3.13.0",
        "google-cloud-pubsub==2.18.0",
        "python-dateutil==2.8.2",
        "pytz==2023.3",
    ],
    python_requires=">=3.11",
    entry_points={
        "console_scripts": [
            "run-insightstream-pipeline=dataflow.pipeline:run",
        ],
    },
)
