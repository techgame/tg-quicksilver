#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Imports 
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#~ Workspace concept
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class WorkspaceSchema(object):
    sqlObjects = ['ws_log', 'ws_version', 'ws_view']

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
            self.createCheckoutView(cur)
            self.createChangesetLog(cur)

    def createWorking(self, cur):
        cur.execute("""\
            create %(temp)s table if not exists %(ws_version)s (
              versionId INTEGER,
              oid INTEGER primary key,
              revId INTEGER,
              ws_revId INTEGER,
              flags INTEGER
            );""" % self.ns)

    def createCheckoutView(self, cur):
        cur.execute("""\
            create %(temp)s view if not exists %(ws_view)s as 
              select oid, %(payloadCols)s
              from %(ws_version)s as S
                join %(qs_revlog)s using (revId, oid)
            ;""" % self.ns)


    def createChangesetLog(self, cur):
        '''Used ChangesetLog to implement undo/redo in a workspace'''

        cur.execute("""\
            create %(temp)s table if not exists %(ws_log)s (
              seqId INTEGER primary key autoincrement,
              revId INTEGER, 
              oid INTEGER not null,
              ts TIMESTAMP default CURRENT_TIMESTAMP,

              %(payloadDefs)s
              );""" % self.ns);
              
    def dropWorkspace(self, cur):
        ns = self.ns
        cur.execute("drop view if exists %(ws_view)s;" % ns)
        cur.execute("drop table if exists %(ws_log)s;" % ns)
        cur.execute("drop table if exists %(ws_version)s;" % ns)

