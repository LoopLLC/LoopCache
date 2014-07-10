﻿namespace LoopCache.Manager
{
    partial class Form1
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.components = new System.ComponentModel.Container();
            this.btnPushData = new System.Windows.Forms.Button();
            this.btnAddNode = new System.Windows.Forms.Button();
            this.txtPort = new System.Windows.Forms.TextBox();
            this.txtMultiplier = new System.Windows.Forms.TextBox();
            this.lstNodes = new System.Windows.Forms.ListBox();
            this.btnRemoveNode = new System.Windows.Forms.Button();
            this.groupBox1 = new System.Windows.Forms.GroupBox();
            this.txtHostName = new System.Windows.Forms.TextBox();
            this.groupBox2 = new System.Windows.Forms.GroupBox();
            this.label2 = new System.Windows.Forms.Label();
            this.label1 = new System.Windows.Forms.Label();
            this.btnPull = new System.Windows.Forms.Button();
            this.txtSeed = new System.Windows.Forms.TextBox();
            this.txtStatus = new System.Windows.Forms.TextBox();
            this.txtException = new System.Windows.Forms.TextBox();
            this.txtObjectCount = new System.Windows.Forms.TextBox();
            this.timer1 = new System.Windows.Forms.Timer(this.components);
            this.pnlMaster = new System.Windows.Forms.GroupBox();
            this.btnConnect = new System.Windows.Forms.Button();
            this.txtMasterPort = new System.Windows.Forms.TextBox();
            this.txtMasterHostName = new System.Windows.Forms.TextBox();
            this.chkMultithread = new System.Windows.Forms.CheckBox();
            this.btnClear = new System.Windows.Forms.Button();
            this.groupBox1.SuspendLayout();
            this.groupBox2.SuspendLayout();
            this.pnlMaster.SuspendLayout();
            this.SuspendLayout();
            // 
            // btnPushData
            // 
            this.btnPushData.Location = new System.Drawing.Point(293, 20);
            this.btnPushData.Name = "btnPushData";
            this.btnPushData.Size = new System.Drawing.Size(75, 23);
            this.btnPushData.TabIndex = 0;
            this.btnPushData.Text = "Push";
            this.btnPushData.UseVisualStyleBackColor = true;
            this.btnPushData.Click += new System.EventHandler(this.btnPush_Click);
            // 
            // btnAddNode
            // 
            this.btnAddNode.Location = new System.Drawing.Point(255, 19);
            this.btnAddNode.Name = "btnAddNode";
            this.btnAddNode.Size = new System.Drawing.Size(75, 23);
            this.btnAddNode.TabIndex = 1;
            this.btnAddNode.Text = "Add";
            this.btnAddNode.UseVisualStyleBackColor = true;
            this.btnAddNode.Click += new System.EventHandler(this.btnAddNode_Click);
            // 
            // txtPort
            // 
            this.txtPort.Location = new System.Drawing.Point(87, 19);
            this.txtPort.Multiline = true;
            this.txtPort.Name = "txtPort";
            this.txtPort.Size = new System.Drawing.Size(86, 20);
            this.txtPort.TabIndex = 2;
            this.txtPort.Text = "12345";
            // 
            // txtMultiplier
            // 
            this.txtMultiplier.Location = new System.Drawing.Point(179, 19);
            this.txtMultiplier.Name = "txtMultiplier";
            this.txtMultiplier.Size = new System.Drawing.Size(70, 20);
            this.txtMultiplier.TabIndex = 3;
            this.txtMultiplier.Text = "0.25";
            // 
            // lstNodes
            // 
            this.lstNodes.Font = new System.Drawing.Font("Courier New", 10F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.lstNodes.FormattingEnabled = true;
            this.lstNodes.ItemHeight = 16;
            this.lstNodes.Location = new System.Drawing.Point(6, 54);
            this.lstNodes.Name = "lstNodes";
            this.lstNodes.Size = new System.Drawing.Size(405, 324);
            this.lstNodes.TabIndex = 4;
            // 
            // btnRemoveNode
            // 
            this.btnRemoveNode.Location = new System.Drawing.Point(336, 19);
            this.btnRemoveNode.Name = "btnRemoveNode";
            this.btnRemoveNode.Size = new System.Drawing.Size(75, 23);
            this.btnRemoveNode.TabIndex = 7;
            this.btnRemoveNode.Text = "Remove Node";
            this.btnRemoveNode.UseVisualStyleBackColor = true;
            this.btnRemoveNode.Click += new System.EventHandler(this.btnRemoveNode_Click);
            // 
            // groupBox1
            // 
            this.groupBox1.Controls.Add(this.txtHostName);
            this.groupBox1.Controls.Add(this.txtPort);
            this.groupBox1.Controls.Add(this.btnRemoveNode);
            this.groupBox1.Controls.Add(this.btnAddNode);
            this.groupBox1.Controls.Add(this.lstNodes);
            this.groupBox1.Controls.Add(this.txtMultiplier);
            this.groupBox1.Location = new System.Drawing.Point(12, 67);
            this.groupBox1.Name = "groupBox1";
            this.groupBox1.Size = new System.Drawing.Size(417, 388);
            this.groupBox1.TabIndex = 8;
            this.groupBox1.TabStop = false;
            this.groupBox1.Text = "Ring";
            // 
            // txtHostName
            // 
            this.txtHostName.Location = new System.Drawing.Point(6, 19);
            this.txtHostName.Name = "txtHostName";
            this.txtHostName.Size = new System.Drawing.Size(75, 20);
            this.txtHostName.TabIndex = 8;
            this.txtHostName.Text = "localhost";
            // 
            // groupBox2
            // 
            this.groupBox2.Controls.Add(this.btnClear);
            this.groupBox2.Controls.Add(this.chkMultithread);
            this.groupBox2.Controls.Add(this.label2);
            this.groupBox2.Controls.Add(this.label1);
            this.groupBox2.Controls.Add(this.btnPull);
            this.groupBox2.Controls.Add(this.txtSeed);
            this.groupBox2.Controls.Add(this.txtStatus);
            this.groupBox2.Controls.Add(this.txtException);
            this.groupBox2.Controls.Add(this.txtObjectCount);
            this.groupBox2.Controls.Add(this.btnPushData);
            this.groupBox2.Location = new System.Drawing.Point(435, 12);
            this.groupBox2.Name = "groupBox2";
            this.groupBox2.Size = new System.Drawing.Size(535, 443);
            this.groupBox2.TabIndex = 9;
            this.groupBox2.TabStop = false;
            this.groupBox2.Text = "Test";
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(105, 25);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(38, 13);
            this.label2.TabIndex = 7;
            this.label2.Text = "Count:";
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(6, 25);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(32, 13);
            this.label1.TabIndex = 6;
            this.label1.Text = "Start:";
            // 
            // btnPull
            // 
            this.btnPull.ImageAlign = System.Drawing.ContentAlignment.BottomLeft;
            this.btnPull.Location = new System.Drawing.Point(374, 20);
            this.btnPull.Name = "btnPull";
            this.btnPull.Size = new System.Drawing.Size(75, 23);
            this.btnPull.TabIndex = 5;
            this.btnPull.Text = "Pull";
            this.btnPull.UseVisualStyleBackColor = true;
            this.btnPull.Click += new System.EventHandler(this.btnPull_Click);
            // 
            // txtSeed
            // 
            this.txtSeed.Location = new System.Drawing.Point(44, 20);
            this.txtSeed.Name = "txtSeed";
            this.txtSeed.Size = new System.Drawing.Size(49, 20);
            this.txtSeed.TabIndex = 4;
            this.txtSeed.Text = "0";
            // 
            // txtStatus
            // 
            this.txtStatus.Font = new System.Drawing.Font("Courier New", 12F, System.Drawing.FontStyle.Regular, System.Drawing.GraphicsUnit.Point, ((byte)(0)));
            this.txtStatus.Location = new System.Drawing.Point(6, 50);
            this.txtStatus.Multiline = true;
            this.txtStatus.Name = "txtStatus";
            this.txtStatus.Size = new System.Drawing.Size(523, 141);
            this.txtStatus.TabIndex = 3;
            // 
            // txtException
            // 
            this.txtException.ForeColor = System.Drawing.Color.Red;
            this.txtException.Location = new System.Drawing.Point(6, 197);
            this.txtException.Multiline = true;
            this.txtException.Name = "txtException";
            this.txtException.Size = new System.Drawing.Size(523, 240);
            this.txtException.TabIndex = 2;
            // 
            // txtObjectCount
            // 
            this.txtObjectCount.Location = new System.Drawing.Point(147, 20);
            this.txtObjectCount.Name = "txtObjectCount";
            this.txtObjectCount.Size = new System.Drawing.Size(49, 20);
            this.txtObjectCount.TabIndex = 1;
            this.txtObjectCount.Text = "10000";
            // 
            // timer1
            // 
            this.timer1.Interval = 1000;
            this.timer1.Tick += new System.EventHandler(this.timer1_Tick);
            // 
            // pnlMaster
            // 
            this.pnlMaster.Controls.Add(this.btnConnect);
            this.pnlMaster.Controls.Add(this.txtMasterPort);
            this.pnlMaster.Controls.Add(this.txtMasterHostName);
            this.pnlMaster.Location = new System.Drawing.Point(12, 12);
            this.pnlMaster.Name = "pnlMaster";
            this.pnlMaster.Size = new System.Drawing.Size(417, 49);
            this.pnlMaster.TabIndex = 11;
            this.pnlMaster.TabStop = false;
            this.pnlMaster.Text = "Master";
            // 
            // btnConnect
            // 
            this.btnConnect.Location = new System.Drawing.Point(337, 20);
            this.btnConnect.Name = "btnConnect";
            this.btnConnect.Size = new System.Drawing.Size(75, 23);
            this.btnConnect.TabIndex = 2;
            this.btnConnect.Text = "Connect";
            this.btnConnect.UseVisualStyleBackColor = true;
            this.btnConnect.Click += new System.EventHandler(this.btnConnect_Click);
            // 
            // txtMasterPort
            // 
            this.txtMasterPort.Location = new System.Drawing.Point(179, 20);
            this.txtMasterPort.Name = "txtMasterPort";
            this.txtMasterPort.Size = new System.Drawing.Size(151, 20);
            this.txtMasterPort.TabIndex = 1;
            this.txtMasterPort.Text = "12345";
            // 
            // txtMasterHostName
            // 
            this.txtMasterHostName.Location = new System.Drawing.Point(6, 20);
            this.txtMasterHostName.Name = "txtMasterHostName";
            this.txtMasterHostName.Size = new System.Drawing.Size(167, 20);
            this.txtMasterHostName.TabIndex = 0;
            this.txtMasterHostName.Text = "localhost";
            // 
            // chkMultithread
            // 
            this.chkMultithread.AutoSize = true;
            this.chkMultithread.Checked = true;
            this.chkMultithread.CheckState = System.Windows.Forms.CheckState.Checked;
            this.chkMultithread.Location = new System.Drawing.Point(209, 24);
            this.chkMultithread.Name = "chkMultithread";
            this.chkMultithread.Size = new System.Drawing.Size(78, 17);
            this.chkMultithread.TabIndex = 8;
            this.chkMultithread.Text = "Multithread";
            this.chkMultithread.UseVisualStyleBackColor = true;
            // 
            // btnClear
            // 
            this.btnClear.Location = new System.Drawing.Point(454, 20);
            this.btnClear.Name = "btnClear";
            this.btnClear.Size = new System.Drawing.Size(75, 23);
            this.btnClear.TabIndex = 9;
            this.btnClear.Text = "Clear";
            this.btnClear.UseVisualStyleBackColor = true;
            this.btnClear.Click += new System.EventHandler(this.btnClear_Click);
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(982, 489);
            this.Controls.Add(this.pnlMaster);
            this.Controls.Add(this.groupBox2);
            this.Controls.Add(this.groupBox1);
            this.Name = "Form1";
            this.Text = "Loop Cache Manager";
            this.groupBox1.ResumeLayout(false);
            this.groupBox1.PerformLayout();
            this.groupBox2.ResumeLayout(false);
            this.groupBox2.PerformLayout();
            this.pnlMaster.ResumeLayout(false);
            this.pnlMaster.PerformLayout();
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.Button btnPushData;
        private System.Windows.Forms.Button btnAddNode;
        private System.Windows.Forms.TextBox txtPort;
        private System.Windows.Forms.TextBox txtMultiplier;
        private System.Windows.Forms.ListBox lstNodes;
        private System.Windows.Forms.Button btnRemoveNode;
        private System.Windows.Forms.GroupBox groupBox1;
        private System.Windows.Forms.GroupBox groupBox2;
        private System.Windows.Forms.TextBox txtException;
        private System.Windows.Forms.TextBox txtObjectCount;
        private System.Windows.Forms.Timer timer1;
        private System.Windows.Forms.TextBox txtHostName;
        private System.Windows.Forms.TextBox txtStatus;
        private System.Windows.Forms.GroupBox pnlMaster;
        private System.Windows.Forms.TextBox txtMasterPort;
        private System.Windows.Forms.TextBox txtMasterHostName;
        private System.Windows.Forms.Button btnConnect;
        private System.Windows.Forms.TextBox txtSeed;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.Button btnPull;
        private System.Windows.Forms.CheckBox chkMultithread;
        private System.Windows.Forms.Button btnClear;
    }
}

