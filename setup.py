import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="ods",
    version="0.0.3",
    license='MIT',
    author="Open Data Studio",
    author_email="moon@staroid.com",
    description="Python client library for Staroid cloud platform",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/open-datastudio/ods",
    packages=setuptools.find_packages(),
    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        # Indicate who your project is intended for
        'Development Status :: 5 - Production/Stable',

        # Indicate who your project is intended for
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries',


        # Specify the Python versions you support here. In particular, ensure
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',

        # Pick your license as you wish (should match "license" above)
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=[
        'staroid',
        'findspark',
        'python3-wget',
        'kubernetes'
    ]
)
