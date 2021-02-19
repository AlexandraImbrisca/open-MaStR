# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys
sys.path.insert(0, os.path.abspath('../open_mastr'))

from open_mastr.soap_api.metadata.create import column_docs_csv

# Preparing data for the documentation
def _generate_data_docs():
    technologies = ["solar", "wind", "biomass", "combustion", "gsgk", "hydro", "nuclear", "storage"]
    raw_data_doc_files = column_docs_csv(technologies,
                                         "data/raw/")

    raw_data_string = "Raw data\n========\n\nRaw data retrieved from MaStR database is structured as follows\n\n"

    for tech, data_table_doc in zip(technologies, raw_data_doc_files):
        section = f"{tech}\n-------\n\n"

        csv_include = f".. csv-table::\n" \
        f"   :file: {os.path.join('raw', data_table_doc)}\n" \
        "   :widths: 20, 35, 15, 15\n" \
        "   :header-rows: 1\n\n\n"

        raw_data_string += section + csv_include
    with open("data/raw_data.rst", "w") as raw_data_fh:
        raw_data_fh.write(raw_data_string)
_generate_data_docs()



# -- Project information -----------------------------------------------------

project = 'open-MaStR'
copyright = '2020, Ludee,gplssm'
author = 'Ludee,gplssm'


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autosectionlabel',
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
    'm2r2',
]

source_suffix = [".rst", ".md"]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'sphinx_rtd_theme'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']
html_css_files = ['custom.css']

# Autodoc config 
autoclass_content = 'both'
