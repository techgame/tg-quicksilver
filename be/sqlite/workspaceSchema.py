#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Workspace concept
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class WorkspaceSchema(object):
    sqlObjects = ['ws_log', 'ws_version']

    def __init__(self, ns, workspace):
        self.ns = ns
        self.initNS(ns, workspace)

    def initNS(self, ns, workspace):
        ns.wsid = workspace.wsid
        if workspace.temporary:
            ns.temp = 'TEMP' 
        else: ns.temp = '' 

        csName = '%(name)s_%(wsid)s' % ns
        for k in self.sqlObjects:
            ns[k] = '%s_%s' % (csName, k)

    def initStore(self, conn):
        with conn:
            cur = conn.cursor()
            self.createWorking(cur)
            self.createChangesetLog(cur)

    def createWorking(self, cur):
        cur.execute("""\
            create %(temp)s table if not exists %(ws_version)s (
              oid INTEGER primary key,
              revId INTEGER,
              versionId INTEGER,
              flags INTEGER,

              ws_versionId INTEGER,
              ws_revId INTEGER,
              seqId INTEGER);""" % self.ns)

    def createChangesetLog(self, cur):
        '''Used ChangesetLog to implement undo/redo in a workspace'''

        cur.execute("""\
            create %(temp)s table if not exists %(ws_log)s (
              oid INTEGER not null,
              revId INTEGER, 

              seqId INTEGER primary key autoincrement,
              grpId INTEGER,

              %(payloadDefs)s);""" % self.ns);

        cur.execute("""\
            create index if not exists %(ws_log)s_index 
                on %(ws_log)s (oid);""" % self.ns)
              
    def dropWorkspace(self, cur):
        ns = self.ns
        cur.execute("drop table if exists %(ws_log)s;" % ns)
        cur.execute("drop table if exists %(ws_version)s;" % ns)

