==========
rc_crawler
==========

Analysis of the results of 13th Malaysian General Election

    sudo apt-get install tesseract-ocr
    sudo apt-get install libjpeg-dev
    echo "tessedit_char_whitelist ABCDEFGHIJKLMNOPQRSTUVWXYZ" | sudo tee /usr/share/tesseract-ocr/tessdata/configs/uppercase_letters

Using Paver
-----------

The ``pavement.py`` file comes with a number of tasks already set up for you. You can see a full list by typing ``paver help`` in the project root directory. The following are included::

    Tasks from pavement:
    lint             - Perform PEP8 style check, run PyFlakes, and run McCabe complexity metrics on the code.
    doc_open         - Build the HTML docs and open them in a web browser.
    coverage         - Run tests and show test coverage report.
    doc_watch        - Watch for changes in the Sphinx documentation and rebuild when changed.
    test             - Run the unit tests.
    get_tasks        - Get all paver-defined tasks.
    commit           - Commit only if all the tests pass.
    test_all         - Perform a style check and run all unit tests.

For example, to run the both the unit tests and lint, run the following in the project root directory::

    paver test_all

To build the HTML documentation, then open it in a web browser::

    paver doc_open

Authors
-------

* Wong Jiang Fung
