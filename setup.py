from setuptools import setup, find_packages
 
setup(
    name="producto_transacciones_fraude",
    version="0.0.1",
    author="Diego Sanchez",
    description="Curado tabla raw_prueba_nequi_input",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    keywords="data_engineering",
    license="BSD 3-Clause License",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.8",
)
 
 