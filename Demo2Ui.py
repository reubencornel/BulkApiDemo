import Tkinter as tk
import ttk
from threading import Thread
import os
from BulkClient import BulkApiOrchestrator, BulkV1Client, BulkV2Client

AUTHENTICATING = "Authenticating"
CREATE_JOB = "CreateJob"
CREATE_BATCHES = "CreateBatches"
CLOSE_JOB = "CloseJob"

class Application(tk.Frame):
    def __init__(self, master = None):
        tk.Frame.__init__(self, master)
        self.pack()

        self.createWidgets()
        self.default_font=("Helvetica", "16")
        self.code_font=("Courier", "16")

    def createWidgets(self):
        top=self.winfo_toplevel()

        top.rowconfigure(0, weight=1)
        top.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)
        self.columnconfigure(0, weight=1)

        self.tabs = ttk.Notebook()
        self.buildDemo1(self.tabs)
        self.tabs.pack()

    def buildDemo1(self, tabs):
        demo0Frame = ttk.Frame(height=200, width=200)
        demo0Frame.pack()

        demo1Frame = ttk.Frame(demo0Frame)
        demo1Frame.pack()

        demo2Frame = ttk.Frame(demo0Frame)
        demo2Frame.pack()

        self.v1Pane = BulkApiDemoPane(demo2Frame, 2, 1, self.v1PaneCallBack)
        self.v2Pane = BulkApiDemoPane(demo2Frame, 2, 3, self.v2PaneCallBack)

        runDemoButton = ttk.Button(demo1Frame, text='Run Demo 1', command = self.runDemo)
        runDemoButton.grid(row=1, column=2)

        tabs.add(demo0Frame, text="Basic Api Demo")

    def runDemo(self):
        self.v1Orchestrator = BulkApiOrchestrator(self.v1Pane, BulkV1Client())
        self.v2Orchestrator = BulkApiOrchestrator(self.v2Pane, BulkV2Client())

        t1 = Thread(target = self.v1Orchestrator.runDemo)
        t2 = Thread(target = self.v2Orchestrator.runDemo)

        t1.start()
        t2.start()

    def v1PaneCallBack(self, event):
        currentSelection = event.widget.selection()[0];
        if (currentSelection == CREATE_JOB):
            self.v1Pane.label.config(justify = tk.CENTER);
            self.v1Pane.label.config(text="Post Job Representation to /services/async/38.0/job");
        elif (currentSelection == CREATE_BATCHES):
            self.v1Pane.label.config(justify=tk.LEFT);
            self.v1Pane.label.config(text="For all lines in the file\n"+
                                     "     Validate if the line meets classic bulk parameters\n" +
                                     "     Validate if we have not hit the batch size\n" +
                                     "     Post to /services/async/38.0/job/jobId/batch");
        elif (currentSelection == CLOSE_JOB):
            self.v1Pane.label.config(justify = tk.CENTER);
            self.v1Pane.label.config(text="Post Job Representation to /services/async/38.0/job/jobId");

    def v2PaneCallBack(self, event):
        currentSelection = event.widget.selection()[0];
        if (currentSelection == CREATE_JOB):
            self.v2Pane.label.config(justify = tk.CENTER);
            self.v2Pane.label.config(text="Post Job Representation to /services/data/v38.0/bulk/jobs");
        elif (currentSelection == CREATE_BATCHES):
            self.v2Pane.label.config(justify=tk.CENTER);
            self.v2Pane.label.config(text="Post Batch File to\n/services/data/v38.0/bulk/jobs/jobId/batches");
        elif (currentSelection == CLOSE_JOB):
            self.v2Pane.label.config(justify = tk.CENTER);
            self.v2Pane.label.config(text="Patch Job Representation at \n /services/data/v38.0/bulk/jobs/jobId");


class BulkApiDemoPane:
    def __init__(self, frame, row, column, bindingFunction=None):
        self.row = row;
        self.column = column;
        self.frame = frame;
        self.bindingFunction = bindingFunction
        self.attentionImage = tk.PhotoImage(file=os.getcwd() + "/Attention.gif", width=20, height=20);
        self.checkedImage = tk.PhotoImage(file=os.getcwd() + "/Checked.gif", width=20, height=20);
        self.treeView = self.buildTreeView()
        self.label = self.buildLabelWidgets();

    def buildTreeView(self):
        tree = ttk.Treeview(self.frame, height=20)
        tree.grid(row=self.row, column=self.column, padx=5, pady=5)
        tree["columns"] = ("Time");
        tree.column("Time", width=200);
        tree.heading("Time", text="Elapsed Time (msec)");
        tree.insert("", "end", iid=AUTHENTICATING, text=" Authenticating", image=self.attentionImage, tags="Header")
        tree.insert("", "end", iid=CREATE_JOB, text=" Create Job", image=self.attentionImage, tags="Header")
        tree.insert("", "end", iid=CREATE_BATCHES, text=" Create Batches", image=self.attentionImage, tags="Header")
        tree.insert("", "end", iid=CLOSE_JOB, text=" Close Job", tags="Header", image=self.attentionImage)
        tree.tag_configure("Header", None, font=("Helvetica", "14"))
        tree.bind('<<TreeviewSelect>>', self.bindingFunction)
        return tree

    def buildLabelWidgets(self):
        label = tk.Label(self.frame,height=10, width=45)
        label.grid(row=(self.row + 1), column=self.column, pady=5)
        return label

    def markStageSuccess(self, stageName):
        self.treeView.item(stageName, None, image=self.checkedImage)
        total = 0;
        for i in self.treeView.get_children(stageName):
            valueToBeAdded = self.treeView.item(i)["values"][0]
            total = total + int(valueToBeAdded)
        self.treeView.item(stageName, None, values=(str(total)))

    def insertItem(self, stage, objectId, time):
        self.treeView.insert(stage, "end", text=objectId, iid=objectId, values=(str(time)))

if __name__ == "__main__":
    app = Application()
    app.master.title('Bulk Api Demo')
    app.mainloop()
