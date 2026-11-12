"""Packages the `dataflow` code so Dataflow workers can import it (--setup_file)."""

from setuptools import setup, find_packages

setup(
    name="insightstream-dataflow",
    version="1.0.0",
    description="InsightStream Dataflow streaming pipeline",
    author="Data Engineering Team",
    packages=find_packages(include=["dataflow", "dataflow.*"]),
    install_requires=[
        "apache-beam[gcp]==2.77.0",
        "google-cloud-bigquery==3.47.0",
        "google-cloud-pubsub==2.44.0",
    ],
    python_requires=">=3.11",
    entry_points={
        "console_scripts": [
            "run-insightstream-pipeline=dataflow.pipeline:run",
        ],
    },
)
