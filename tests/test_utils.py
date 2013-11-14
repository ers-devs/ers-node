# Prefer ../ers-local/ers/ers.py over installed versions
# TODO: Use setuptools for testing without installing
import os, sys
TESTS_PATH = os.path.dirname(os.path.realpath(__file__))
ERS_PATH = os.path.join(os.path.dirname(TESTS_PATH), 'ers-local')
sys.path.insert(0, ERS_PATH)

from ers import ERSLocal
from ers.utils import import_nt, import_nt_rdflib

nt_file = os.path.join(TESTS_PATH, 'data', 'timbl.nt')

def test(keep_db=False):
    dbname = 'ers_test_utils'
    ers = ERSLocal(dbname=dbname, reset_database=True)
    import_nt(ers, nt_file, 'timbl')
    assert ers.exist('http://www4.wiwiss.fu-berlin.de/booksMeshup/books/006251587X', 'timbl') == True
    import_nt_rdflib(ers, nt_file, 'timbl2')
    assert ers.exist('http://www4.wiwiss.fu-berlin.de/booksMeshup/books/006251587X', 'timbl2') == True
    assert ers.exist('http://www4.wiwiss.fu-berlin.de/booksMeshup/books/006251587X', 'timbl3') == False
    print "Tests pass"
    if not keep_db:
        ers.db.server.delete_db(dbname)

if __name__ == '__main__':
    test()
