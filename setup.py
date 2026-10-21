"""Empacota o módulo comum para os workers do Dataflow (usado via --setup_file)."""
import setuptools

setuptools.setup(
    name="ballon-common",
    version="1.0.0",
    description="Regras compartilhadas do preditor Ballon d'Or (Trabalho 2)",
    packages=setuptools.find_packages(include=["ballon_common", "ballon_common.*"]),
    install_requires=[],  # só biblioteca padrão; o Beam já vem na imagem do Dataflow
)
