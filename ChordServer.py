import glob
import sys
sys.path.append('gen-py')
sys.path.insert(0, glob.glob('/home/yaoliu/src_code/local/lib/lib/python2.7/site-packages')[0])

from chord import FileStore
from chord.ttypes import SystemException
from chord.ttypes import NodeID, RFileMetadata, RFile

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

import hashlib
from hashlib import sha256
import socket


class FileStoreHandler:
    def __init__(self):
        self.log = {}
        #self.ip = socket.gethostbyname(socket.gethostname());
        #self.port = int(sys.argv[1]);
        self.files = {};    #filename: RFile
        self.node = NodeID();
        self.node.ip = str(socket.gethostbyname(socket.gethostname()));
        self.node.port = int(sys.argv[1]);
        print(self.node.ip + ":" + str(self.node.port));
        self.fingerTable = [];
        line = (str(self.node.ip)+":"+sys.argv[1]).encode("utf-8");
        self.node.id = str(sha256(line).hexdigest());
        # print("self: "+self.node.id);


    def writeFile(self, rFile):
        key = sha256(rFile.meta.filename.encode("utf-8")).hexdigest();
        succ = self.findSucc(key);

        if succ.id == self.node.id:
            if not rFile.meta.filename in self.files:
                x = RFile();

                rFile.meta.version = 0;
                rFile.meta.contentHash = sha256(rFile.content.encode("utf-8")).hexdigest();
                x = rFile;
                self.files[rFile.meta.filename] = x;

                # self.files[rFile.meta.filename] = rFile.meta;
                # print("rFile: "+rFile.meta.filename);

            else:
                rFile.meta.version = self.files[rFile.meta.filename].meta.version + 1;
                rFile.meta.contentHash = sha256(rFile.content.encode("utf-8")).hexdigest();
                self.files[rFile.meta.filename] = rFile;
        else:
            raise SystemException(self.node.ip+":"+str(self.node.port)+" Server doesn't has the right to write "+rFile.meta.filename+" file.");
        # return ''

    def readFile(self, fileName):
        # print("fileName: "+fileName);

        key = sha256(fileName.encode("utf-8")).hexdigest();
        succ = self.findSucc(key);

        if succ.id == self.node.id:
            if fileName in self.files:
                return self.files[fileName];
            else:
                raise SystemException(self.node.ip+":"+str(self.node.port)+" Server doesn't has the requested "+fileName+" file.");

        else:
            raise SystemException(self.node.ip+":"+str(self.node.port)+" Server doesn't has the right to read "+fileName+" file.");

    def setFingertable(self, nodeList):
        # print("\n\nFingerTable")
        # for n in nodeList:
        #     print("nodelist: "+n.id);
        self.fingerTable = nodeList;

    def findSucc(self, key):
        print("In findSucc")
        if not self.fingerTable:
            raise SystemException("Finger table is Empty");

        if key == self.node.id:
            print("Out findSucc")
            return self.node;

        pred = self.findPred(key);
        if key == pred.id:
            print("Out findSucc")
            return pred;
        else:
            if pred.id == self.node.id:
                succ1 = self.getNodeSucc();
                print("Out findSucc")
                return succ1;

            t = TSocket.TSocket(pred.ip, pred.port);
            pf = TBinaryProtocol.TBinaryProtocol(t);
            newNodeClient = FileStore.Client(pf);
            t.open();
            succ = newNodeClient.getNodeSucc();
            t.close();
            print("Out findSucc")
            return succ;

    def findPred(self, key):
        print("In findPred")
        if not self.fingerTable:
            raise SystemException("Finger table is Empty");

        getNode = self.getNodeSucc();

        # print("Hello: "+getNode.ip);
        # if int(key) in range(int(self.node.id), int(getNode.id)):
        #     return getNode;

        if (self.node.id < getNode.id):
            if (key > self.node.id) and (key <= getNode.id):
                print("Out findPred")
                return self.node;
        else:
            if (key > self.node.id) or (key <= getNode.id):
                print("Out findPred")
                return self.node;

        getNode = self.closestPred(key);

        t1 = TSocket.TSocket(getNode.ip, getNode.port);
        pf1 = TBinaryProtocol.TBinaryProtocol(t1);
        newNodeClient = FileStore.Client(pf1);
        t1.open();
        # s = transport.client.findSucc(key);

        pred = newNodeClient.findPred(key);
        t1.close();
        print("Out findPred")
        return pred;

    def closestPred(self, key):
        print("In closestPred")
        fingetTableLen = len(self.fingerTable)-1;
        for i in range(fingetTableLen, 0, -1):
            # print(i)
            if self.node.id < key:
                if (self.fingerTable[i].id > self.node.id) and (self.fingerTable[i].id < key):
                    print("Out closestPred")
                    return self.fingerTable[i];
            else:
                if self.fingerTable[i].id > self.node.id:
                    print("Out closestPred")
                    return self.fingerTable[i];
                elif self.fingerTable[i].id < key:
                    print("Out closestPred")
                    return self.fingerTable[i];

        print("Out closestPred")
        return self.node;

    def getNodeSucc(self):
        print("In getNodeSucc")
        if not self.fingerTable:
            raise SystemException("Finger table is Empty");
        print("Out getNodeSucc")
        return self.fingerTable[0];

if __name__ == '__main__':
    handler = FileStoreHandler()
    processor = FileStore.Processor(handler)
    # port = int(sys.argv[1]);
    transport = TSocket.TServerSocket(port = int(sys.argv[1]))
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)

    print('Starting the server...')
    server.serve()
    print('done.')