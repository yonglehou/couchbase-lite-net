/**
 * Couchbase Lite for .NET
 *
 * Original iOS version by Jens Alfke
 * Android Port by Marty Schoch, Traun Leyden
 * C# Port by Zack Gramana
 *
 * Copyright (c) 2012, 2013 Couchbase, Inc. All rights reserved.
 * Portions (c) 2013 Xamarin, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Android.Content;
using Android.Database;
using Android.Database.Sqlite;
using Android.Text;
using Android.Util;
using Couchbase;
using Couchbase.Internal;
using Couchbase.Replicator;
using Couchbase.Support;
using Sharpen;

namespace Couchbase
{
	/// <summary>A CouchbaseLite database.</summary>
	/// <remarks>A CouchbaseLite database.</remarks>
	public class CBLDatabase : Observable
	{
		private string path;

		private string name;

		private SQLiteDatabase sqliteDb;

		private bool open = false;

		private int transactionLevel = 0;

		public const string Tag = "CBLDatabase";

		private IDictionary<string, CBLView> views;

		private IDictionary<string, CBLFilterBlock> filters;

		private IDictionary<string, CBLFilterCompiler> filterCompiler;

		private IDictionary<string, CBLValidationBlock> validations;

		private IDictionary<string, CBLBlobStoreWriter> pendingAttachmentsByDigest;

		private IList<CBLReplicator> activeReplicators;

		private CBLBlobStore attachments;

		private CBLManager manager;

		private CBLDatabaseInternal dbInternal;

		public static int kBigAttachmentLength = (16 * 1024);

		/// <summary>Options for what metadata to include in document bodies</summary>
		public enum TDContentOptions
		{
			TDIncludeAttachments,
			TDIncludeConflicts,
			TDIncludeRevs,
			TDIncludeRevsInfo,
			TDIncludeLocalSeq,
			TDNoBody,
			TDBigAttachmentsFollow
		}

		private static readonly ICollection<string> KnownSpecialKeys;

		static CBLDatabase()
		{
			// Length that constitutes a 'big' attachment
			KnownSpecialKeys = new HashSet<string>();
			KnownSpecialKeys.AddItem("_id");
			KnownSpecialKeys.AddItem("_rev");
			KnownSpecialKeys.AddItem("_attachments");
			KnownSpecialKeys.AddItem("_deleted");
			KnownSpecialKeys.AddItem("_revisions");
			KnownSpecialKeys.AddItem("_revs_info");
			KnownSpecialKeys.AddItem("_conflicts");
			KnownSpecialKeys.AddItem("_deleted_conflicts");
		}

		public const string Schema = string.Empty + "CREATE TABLE docs ( " + "        doc_id INTEGER PRIMARY KEY, "
			 + "        docid TEXT UNIQUE NOT NULL); " + "    CREATE INDEX docs_docid ON docs(docid); "
			 + "    CREATE TABLE revs ( " + "        sequence INTEGER PRIMARY KEY AUTOINCREMENT, "
			 + "        doc_id INTEGER NOT NULL REFERENCES docs(doc_id) ON DELETE CASCADE, "
			 + "        revid TEXT NOT NULL, " + "        parent INTEGER REFERENCES revs(sequence) ON DELETE SET NULL, "
			 + "        current BOOLEAN, " + "        deleted BOOLEAN DEFAULT 0, " + "        json BLOB); "
			 + "    CREATE INDEX revs_by_id ON revs(revid, doc_id); " + "    CREATE INDEX revs_current ON revs(doc_id, current); "
			 + "    CREATE INDEX revs_parent ON revs(parent); " + "    CREATE TABLE localdocs ( "
			 + "        docid TEXT UNIQUE NOT NULL, " + "        revid TEXT NOT NULL, " + "        json BLOB); "
			 + "    CREATE INDEX localdocs_by_docid ON localdocs(docid); " + "    CREATE TABLE views ( "
			 + "        view_id INTEGER PRIMARY KEY, " + "        name TEXT UNIQUE NOT NULL,"
			 + "        version TEXT, " + "        lastsequence INTEGER DEFAULT 0); " + "    CREATE INDEX views_by_name ON views(name); "
			 + "    CREATE TABLE maps ( " + "        view_id INTEGER NOT NULL REFERENCES views(view_id) ON DELETE CASCADE, "
			 + "        sequence INTEGER NOT NULL REFERENCES revs(sequence) ON DELETE CASCADE, "
			 + "        key TEXT NOT NULL COLLATE JSON, " + "        value TEXT); " + "    CREATE INDEX maps_keys on maps(view_id, key COLLATE JSON); "
			 + "    CREATE TABLE attachments ( " + "        sequence INTEGER NOT NULL REFERENCES revs(sequence) ON DELETE CASCADE, "
			 + "        filename TEXT NOT NULL, " + "        key BLOB NOT NULL, " + "        type TEXT, "
			 + "        length INTEGER NOT NULL, " + "        revpos INTEGER DEFAULT 0); " +
			 "    CREATE INDEX attachments_by_sequence on attachments(sequence, filename); "
			 + "    CREATE TABLE replicators ( " + "        remote TEXT NOT NULL, " + "        push BOOLEAN, "
			 + "        last_sequence TEXT, " + "        UNIQUE (remote, push)); " + "    PRAGMA user_version = 3";

		// at the end, update user_version
		/// <summary>The database manager that owns this database.</summary>
		/// <remarks>The database manager that owns this database.</remarks>
		public virtual CBLManager GetManager()
		{
			return manager;
		}

		public virtual Uri GetInternalURL()
		{
			// TODO: implement this
			throw new RuntimeException("Not implemented");
		}

		/// <summary>Constructor</summary>
		/// <param name="path"></param>
		/// <param name="manager"></param>
		public CBLDatabase(string path, CBLManager manager)
		{
			System.Diagnostics.Debug.Assert((path.StartsWith("/")));
			//path must be absolute
			this.path = path;
			this.name = FileDirUtils.GetDatabaseNameFromPath(path);
			this.manager = manager;
		}

		/// <summary>Instantiates a CBLDocument object with the given ID.</summary>
		/// <remarks>
		/// Instantiates a CBLDocument object with the given ID.
		/// Doesn't touch the on-disk sqliteDb; a document with that ID doesn't
		/// even need to exist yet. CBLDocuments are cached, so there will
		/// never be more than one instance (in this sqliteDb) at a time with
		/// the same documentID.
		/// NOTE: the caching described above is not implemented yet
		/// </remarks>
		/// <param name="documentId"></param>
		/// <returns></returns>
		public virtual CBLDocument GetDocument(string documentId)
		{
			// TODO: try to get from cache first
			if (documentId == null || documentId.Length == 0)
			{
				return null;
			}
			return new CBLDocument(this, documentId);
		}

		/// <summary>Creates a new CBLDocument object with no properties and a new (random) UUID.
		/// 	</summary>
		/// <remarks>
		/// Creates a new CBLDocument object with no properties and a new (random) UUID.
		/// The document will be saved to the database when you call -putProperties: on it.
		/// </remarks>
		public virtual CBLDocument GetUntitledDocument()
		{
			return GetDocument(CBLMisc.TDCreateUUID());
		}

		/// <summary>Returns the already-instantiated cached CBLDocument with the given ID, or nil if none is yet cached.
		/// 	</summary>
		/// <remarks>Returns the already-instantiated cached CBLDocument with the given ID, or nil if none is yet cached.
		/// 	</remarks>
		public virtual CBLDocument GetCachedDocument(string documentID)
		{
			// TODO: implement
			throw new RuntimeException("Not implemented");
		}

		/// <summary>Empties the cache of recently used CBLDocument objects.</summary>
		/// <remarks>
		/// Empties the cache of recently used CBLDocument objects.
		/// API calls will now instantiate and return new instances.
		/// </remarks>
		public virtual void ClearDocumentCache()
		{
			// TODO: implement
			throw new RuntimeException("Not implemented");
		}

		internal virtual void RemoveDocumentFromCache(CBLDocument document)
		{
		}

		// TODO: implement
		public override string ToString()
		{
			return this.GetType().FullName + "[" + path + "]";
		}

		public virtual bool Exists()
		{
			return new FilePath(path).Exists();
		}

		/// <summary>Replaces the sqliteDb with a copy of another sqliteDb.</summary>
		/// <remarks>
		/// Replaces the sqliteDb with a copy of another sqliteDb.
		/// This is primarily used to install a canned sqliteDb on first launch of an app, in which case you should
		/// first check .exists to avoid replacing the sqliteDb if it exists already. The canned sqliteDb
		/// would have been copied into your app bundle at build time.
		/// </remarks>
		/// <param name="databasePath">Path of the sqliteDb file that should replace this one.
		/// 	</param>
		/// <param name="attachmentsPath">Path of the associated attachments directory, or nil if there are no attachments.
		/// 	</param>
		/// <returns>true if the sqliteDb was copied, IOException if an error occurs</returns>
		/// <exception cref="System.IO.IOException"></exception>
		public virtual bool ReplaceWithDatabase(string databasePath, string attachmentsPath
			)
		{
			string dstAttachmentsPath = this.GetAttachmentStorePath();
			FilePath sourceFile = new FilePath(databasePath);
			FilePath destFile = new FilePath(path);
			FileDirUtils.CopyFile(sourceFile, destFile);
			FilePath attachmentsFile = new FilePath(dstAttachmentsPath);
			FileDirUtils.DeleteRecursive(attachmentsFile);
			attachmentsFile.Mkdirs();
			if (attachmentsPath != null)
			{
				FileDirUtils.CopyFolder(new FilePath(attachmentsPath), attachmentsFile);
			}
			return true;
		}

		public virtual string GetAttachmentStorePath()
		{
			string attachmentStorePath = path;
			int lastDotPosition = attachmentStorePath.LastIndexOf('.');
			if (lastDotPosition > 0)
			{
				attachmentStorePath = Sharpen.Runtime.Substring(attachmentStorePath, 0, lastDotPosition
					);
			}
			attachmentStorePath = attachmentStorePath + FilePath.separator + "attachments";
			return attachmentStorePath;
		}

		public static Couchbase.CBLDatabase CreateEmptyDBAtPath(string path, CBLManager manager
			)
		{
			if (!FileDirUtils.RemoveItemIfExists(path))
			{
				return null;
			}
			Couchbase.CBLDatabase result = new Couchbase.CBLDatabase(path, manager);
			FilePath af = new FilePath(result.GetAttachmentStorePath());
			//recursively delete attachments path
			if (!FileDirUtils.DeleteRecursive(af))
			{
				return null;
			}
			if (!result.Open())
			{
				return null;
			}
			return result;
		}

		public virtual bool Initialize(string statements)
		{
			try
			{
				foreach (string statement in statements.Split(";"))
				{
					sqliteDb.ExecSQL(statement);
				}
			}
			catch (SQLException)
			{
				Close();
				return false;
			}
			return true;
		}

		public virtual bool Open()
		{
			if (open)
			{
				return true;
			}
			try
			{
				sqliteDb = SQLiteDatabase.OpenDatabase(path, null, SQLiteDatabase.CreateIfNecessary
					);
				TDCollateJSON.RegisterCustomCollators(sqliteDb);
			}
			catch (SQLiteException e)
			{
				Log.E(Couchbase.CBLDatabase.Tag, "Error opening", e);
				return false;
			}
			dbInternal = new CBLDatabaseInternal(this, sqliteDb);
			// Stuff we need to initialize every time the sqliteDb opens:
			if (!Initialize("PRAGMA foreign_keys = ON;"))
			{
				Log.E(Couchbase.CBLDatabase.Tag, "Error turning on foreign keys");
				return false;
			}
			// Check the user_version number we last stored in the sqliteDb:
			int dbVersion = sqliteDb.GetVersion();
			// Incompatible version changes increment the hundreds' place:
			if (dbVersion >= 100)
			{
				Log.W(Couchbase.CBLDatabase.Tag, "CBLDatabase: Database version (" + dbVersion + 
					") is newer than I know how to work with");
				sqliteDb.Close();
				return false;
			}
			if (dbVersion < 1)
			{
				// First-time initialization:
				// (Note: Declaring revs.sequence as AUTOINCREMENT means the values will always be
				// monotonically increasing, never reused. See <http://www.sqlite.org/autoinc.html>)
				if (!Initialize(Schema))
				{
					sqliteDb.Close();
					return false;
				}
				dbVersion = 3;
			}
			if (dbVersion < 2)
			{
				// Version 2: added attachments.revpos
				string upgradeSql = "ALTER TABLE attachments ADD COLUMN revpos INTEGER DEFAULT 0; "
					 + "PRAGMA user_version = 2";
				if (!Initialize(upgradeSql))
				{
					sqliteDb.Close();
					return false;
				}
				dbVersion = 2;
			}
			if (dbVersion < 3)
			{
				string upgradeSql = "CREATE TABLE localdocs ( " + "docid TEXT UNIQUE NOT NULL, " 
					+ "revid TEXT NOT NULL, " + "json BLOB); " + "CREATE INDEX localdocs_by_docid ON localdocs(docid); "
					 + "PRAGMA user_version = 3";
				if (!Initialize(upgradeSql))
				{
					sqliteDb.Close();
					return false;
				}
				dbVersion = 3;
			}
			if (dbVersion < 4)
			{
				string upgradeSql = "CREATE TABLE info ( " + "key TEXT PRIMARY KEY, " + "value TEXT); "
					 + "INSERT INTO INFO (key, value) VALUES ('privateUUID', '" + CBLMisc.TDCreateUUID
					() + "'); " + "INSERT INTO INFO (key, value) VALUES ('publicUUID',  '" + CBLMisc
					.TDCreateUUID() + "'); " + "PRAGMA user_version = 4";
				if (!Initialize(upgradeSql))
				{
					sqliteDb.Close();
					return false;
				}
			}
			try
			{
				attachments = new CBLBlobStore(GetAttachmentStorePath());
			}
			catch (ArgumentException e)
			{
				Log.E(Couchbase.CBLDatabase.Tag, "Could not initialize attachment store", e);
				sqliteDb.Close();
				return false;
			}
			open = true;
			return true;
		}

		public virtual bool Close()
		{
			if (!open)
			{
				return false;
			}
			if (views != null)
			{
				foreach (CBLView view in views.Values)
				{
					view.DatabaseClosing();
				}
			}
			views = null;
			if (activeReplicators != null)
			{
				foreach (CBLReplicator replicator in activeReplicators)
				{
					replicator.DatabaseClosing();
				}
				activeReplicators = null;
			}
			if (sqliteDb != null && sqliteDb.IsOpen())
			{
				sqliteDb.Close();
			}
			open = false;
			transactionLevel = 0;
			return true;
		}

		/// <summary>Deletes the database.</summary>
		/// <remarks>Deletes the database.</remarks>
		public virtual bool Delete()
		{
			if (open)
			{
				if (!Close())
				{
					return false;
				}
			}
			else
			{
				if (!Exists())
				{
					return true;
				}
			}
			FilePath file = new FilePath(path);
			FilePath attachmentsFile = new FilePath(GetAttachmentStorePath());
			bool deleteStatus = file.Delete();
			//recursively delete attachments path
			bool deleteAttachmentStatus = FileDirUtils.DeleteRecursive(attachmentsFile);
			return deleteStatus && deleteAttachmentStatus;
		}

		public virtual string GetPath()
		{
			return path;
		}

		/// <summary>The database's name.</summary>
		/// <remarks>The database's name.</remarks>
		public virtual string GetName()
		{
			return name;
		}

		public virtual void SetName(string name)
		{
			this.name = name;
		}

		// Leave this package protected, so it can only be used
		// CBLView uses this accessor
		public virtual SQLiteDatabase GetSqliteDb()
		{
			return sqliteDb;
		}

		public virtual CBLBlobStore GetAttachments()
		{
			return attachments;
		}

		public virtual CBLBlobStoreWriter GetAttachmentWriter()
		{
			return new CBLBlobStoreWriter(GetAttachments());
		}

		public virtual long TotalDataSize()
		{
			FilePath f = new FilePath(path);
			long size = f.Length() + attachments.TotalDataSize();
			return size;
		}

		/// <summary>Begins a sqliteDb transaction.</summary>
		/// <remarks>
		/// Begins a sqliteDb transaction. Transactions can nest.
		/// Every beginTransaction() must be balanced by a later endTransaction()
		/// </remarks>
		public virtual bool BeginTransaction()
		{
			try
			{
				sqliteDb.BeginTransaction();
				++transactionLevel;
			}
			catch (SQLException)
			{
				//Log.v(TAG, "Begin transaction (level " + Integer.toString(transactionLevel) + ")...");
				return false;
			}
			return true;
		}

		/// <summary>Commits or aborts (rolls back) a transaction.</summary>
		/// <remarks>Commits or aborts (rolls back) a transaction.</remarks>
		/// <param name="commit">If true, commits; if false, aborts and rolls back, undoing all changes made since the matching -beginTransaction call, *including* any committed nested transactions.
		/// 	</param>
		public virtual bool EndTransaction(bool commit)
		{
			System.Diagnostics.Debug.Assert((transactionLevel > 0));
			if (commit)
			{
				//Log.v(TAG, "Committing transaction (level " + Integer.toString(transactionLevel) + ")...");
				sqliteDb.SetTransactionSuccessful();
				sqliteDb.EndTransaction();
			}
			else
			{
				Log.V(Tag, "CANCEL transaction (level " + Sharpen.Extensions.ToString(transactionLevel
					) + ")...");
				try
				{
					sqliteDb.EndTransaction();
				}
				catch (SQLException)
				{
					return false;
				}
			}
			--transactionLevel;
			return true;
		}

		/// <summary>Runs the block within a transaction.</summary>
		/// <remarks>
		/// Runs the block within a transaction. If the block returns NO, the transaction is rolled back.
		/// Use this when performing bulk write operations like multiple inserts/updates;
		/// it saves the overhead of multiple SQLite commits, greatly improving performance.
		/// Does not commit the transaction if the code throws an Exception.
		/// TODO: the iOS version has a retry loop, so there should be one here too
		/// </remarks>
		/// <param name="databaseFunction"></param>
		public virtual void InTransaction(CBLDatabaseFunction databaseFunction)
		{
			bool shouldCommit = true;
			BeginTransaction();
			try
			{
				shouldCommit = databaseFunction.PerformFunction();
			}
			catch (Exception e)
			{
				shouldCommit = false;
				Log.E(Couchbase.CBLDatabase.Tag, e.ToString(), e);
				throw new RuntimeException(e);
			}
			finally
			{
				EndTransaction(shouldCommit);
			}
		}

		/// <summary>
		/// Compacts the database file by purging non-current revisions, deleting unused attachment files,
		/// and running a SQLite "VACUUM" command.
		/// </summary>
		/// <remarks>
		/// Compacts the database file by purging non-current revisions, deleting unused attachment files,
		/// and running a SQLite "VACUUM" command.
		/// </remarks>
		public virtual CBLStatus Compact()
		{
			// Can't delete any rows because that would lose revision tree history.
			// But we can remove the JSON of non-current revisions, which is most of the space.
			try
			{
				Log.V(Couchbase.CBLDatabase.Tag, "Deleting JSON of old revisions...");
				ContentValues args = new ContentValues();
				args.Put("json", (string)null);
				sqliteDb.Update("revs", args, "current=0", null);
			}
			catch (SQLException e)
			{
				Log.E(Couchbase.CBLDatabase.Tag, "Error compacting", e);
				return new CBLStatus(CBLStatus.InternalServerError);
			}
			Log.V(Couchbase.CBLDatabase.Tag, "Deleting old attachments...");
			CBLStatus result = GarbageCollectAttachments();
			Log.V(Couchbase.CBLDatabase.Tag, "Vacuuming SQLite sqliteDb...");
			try
			{
				sqliteDb.ExecSQL("VACUUM");
			}
			catch (SQLException e)
			{
				Log.E(Couchbase.CBLDatabase.Tag, "Error vacuuming sqliteDb", e);
				return new CBLStatus(CBLStatus.InternalServerError);
			}
			return result;
		}

		public virtual string PrivateUUID()
		{
			string result = null;
			Cursor cursor = null;
			try
			{
				cursor = sqliteDb.RawQuery("SELECT value FROM info WHERE key='privateUUID'", null
					);
				if (cursor.MoveToFirst())
				{
					result = cursor.GetString(0);
				}
			}
			catch (SQLException e)
			{
				Log.E(Tag, "Error querying privateUUID", e);
			}
			finally
			{
				if (cursor != null)
				{
					cursor.Close();
				}
			}
			return result;
		}

		public virtual string PublicUUID()
		{
			string result = null;
			Cursor cursor = null;
			try
			{
				cursor = sqliteDb.RawQuery("SELECT value FROM info WHERE key='publicUUID'", null);
				if (cursor.MoveToFirst())
				{
					result = cursor.GetString(0);
				}
			}
			catch (SQLException e)
			{
				Log.E(Tag, "Error querying privateUUID", e);
			}
			finally
			{
				if (cursor != null)
				{
					cursor.Close();
				}
			}
			return result;
		}

		/// <summary>The number of documents in the database.</summary>
		/// <remarks>The number of documents in the database.</remarks>
		public virtual int GetDocumentCount()
		{
			string sql = "SELECT COUNT(DISTINCT doc_id) FROM revs WHERE current=1 AND deleted=0";
			Cursor cursor = null;
			int result = 0;
			try
			{
				cursor = sqliteDb.RawQuery(sql, null);
				if (cursor.MoveToFirst())
				{
					result = cursor.GetInt(0);
				}
			}
			catch (SQLException e)
			{
				Log.E(Couchbase.CBLDatabase.Tag, "Error getting document count", e);
			}
			finally
			{
				if (cursor != null)
				{
					cursor.Close();
				}
			}
			return result;
		}

		/// <summary>The latest sequence number used.</summary>
		/// <remarks>
		/// The latest sequence number used.  Every new revision is assigned a new sequence number,
		/// so this property increases monotonically as changes are made to the database. It can be
		/// used to check whether the database has changed between two points in time.
		/// </remarks>
		public virtual long GetLastSequence()
		{
			string sql = "SELECT MAX(sequence) FROM revs";
			Cursor cursor = null;
			long result = 0;
			try
			{
				cursor = sqliteDb.RawQuery(sql, null);
				if (cursor.MoveToFirst())
				{
					result = cursor.GetLong(0);
				}
			}
			catch (SQLException e)
			{
				Log.E(Couchbase.CBLDatabase.Tag, "Error getting last sequence", e);
			}
			finally
			{
				if (cursor != null)
				{
					cursor.Close();
				}
			}
			return result;
		}

		/// <summary>Splices the contents of an NSDictionary into JSON data (that already represents a dict), without parsing the JSON.
		/// 	</summary>
		/// <remarks>Splices the contents of an NSDictionary into JSON data (that already represents a dict), without parsing the JSON.
		/// 	</remarks>
		public virtual byte[] AppendDictToJSON(byte[] json, IDictionary<string, object> dict
			)
		{
			if (dict.Count == 0)
			{
				return json;
			}
			byte[] extraJSON = null;
			try
			{
				extraJSON = CBLServer.GetObjectMapper().WriteValueAsBytes(dict);
			}
			catch (Exception e)
			{
				Log.E(Couchbase.CBLDatabase.Tag, "Error convert extra JSON to bytes", e);
				return null;
			}
			int jsonLength = json.Length;
			int extraLength = extraJSON.Length;
			if (jsonLength == 2)
			{
				// Original JSON was empty
				return extraJSON;
			}
			byte[] newJson = new byte[jsonLength + extraLength - 1];
			System.Array.Copy(json, 0, newJson, 0, jsonLength - 1);
			// Copy json w/o trailing '}'
			newJson[jsonLength - 1] = (byte)(',');
			// Add a ','
			System.Array.Copy(extraJSON, 1, newJson, jsonLength, extraLength - 1);
			return newJson;
		}

		/// <summary>Inserts the _id, _rev and _attachments properties into the JSON data and stores it in rev.
		/// 	</summary>
		/// <remarks>
		/// Inserts the _id, _rev and _attachments properties into the JSON data and stores it in rev.
		/// Rev must already have its revID and sequence properties set.
		/// </remarks>
		public virtual IDictionary<string, object> ExtraPropertiesForRevision(CBLRevisionInternal
			 rev, EnumSet<CBLDatabase.TDContentOptions> contentOptions)
		{
			string docId = rev.GetDocId();
			string revId = rev.GetRevId();
			long sequenceNumber = rev.GetSequence();
			System.Diagnostics.Debug.Assert((revId != null));
			System.Diagnostics.Debug.Assert((sequenceNumber > 0));
			// Get attachment metadata, and optionally the contents:
			IDictionary<string, object> attachmentsDict = GetAttachmentsDictForSequenceWithContent
				(sequenceNumber, contentOptions);
			// Get more optional stuff to put in the properties:
			//OPT: This probably ends up making redundant SQL queries if multiple options are enabled.
			long localSeq = null;
			if (contentOptions.Contains(CBLDatabase.TDContentOptions.TDIncludeLocalSeq))
			{
				localSeq = sequenceNumber;
			}
			IDictionary<string, object> revHistory = null;
			if (contentOptions.Contains(CBLDatabase.TDContentOptions.TDIncludeRevs))
			{
				revHistory = GetRevisionHistoryDict(rev);
			}
			IList<object> revsInfo = null;
			if (contentOptions.Contains(CBLDatabase.TDContentOptions.TDIncludeRevsInfo))
			{
				revsInfo = new AList<object>();
				IList<CBLRevisionInternal> revHistoryFull = GetRevisionHistory(rev);
				foreach (CBLRevisionInternal historicalRev in revHistoryFull)
				{
					IDictionary<string, object> revHistoryItem = new Dictionary<string, object>();
					string status = "available";
					if (historicalRev.IsDeleted())
					{
						status = "deleted";
					}
					// TODO: Detect missing revisions, set status="missing"
					revHistoryItem.Put("rev", historicalRev.GetRevId());
					revHistoryItem.Put("status", status);
					revsInfo.AddItem(revHistoryItem);
				}
			}
			IList<string> conflicts = null;
			if (contentOptions.Contains(CBLDatabase.TDContentOptions.TDIncludeConflicts))
			{
				CBLRevisionList revs = GetAllRevisionsOfDocumentID(docId, true);
				if (revs.Count > 1)
				{
					conflicts = new AList<string>();
					foreach (CBLRevisionInternal historicalRev in revs)
					{
						if (!historicalRev.Equals(rev))
						{
							conflicts.AddItem(historicalRev.GetRevId());
						}
					}
				}
			}
			IDictionary<string, object> result = new Dictionary<string, object>();
			result.Put("_id", docId);
			result.Put("_rev", revId);
			if (rev.IsDeleted())
			{
				result.Put("_deleted", true);
			}
			if (attachmentsDict != null)
			{
				result.Put("_attachments", attachmentsDict);
			}
			if (localSeq != null)
			{
				result.Put("_local_seq", localSeq);
			}
			if (revHistory != null)
			{
				result.Put("_revisions", revHistory);
			}
			if (revsInfo != null)
			{
				result.Put("_revs_info", revsInfo);
			}
			if (conflicts != null)
			{
				result.Put("_conflicts", conflicts);
			}
			return result;
		}

		/// <summary>Inserts the _id, _rev and _attachments properties into the JSON data and stores it in rev.
		/// 	</summary>
		/// <remarks>
		/// Inserts the _id, _rev and _attachments properties into the JSON data and stores it in rev.
		/// Rev must already have its revID and sequence properties set.
		/// </remarks>
		public virtual void ExpandStoredJSONIntoRevisionWithAttachments(byte[] json, CBLRevisionInternal
			 rev, EnumSet<CBLDatabase.TDContentOptions> contentOptions)
		{
			IDictionary<string, object> extra = ExtraPropertiesForRevision(rev, contentOptions
				);
			if (json != null)
			{
				rev.SetJson(AppendDictToJSON(json, extra));
			}
			else
			{
				rev.SetProperties(extra);
			}
		}

		public virtual IDictionary<string, object> DocumentPropertiesFromJSON(byte[] json
			, string docId, string revId, long sequence, EnumSet<CBLDatabase.TDContentOptions
			> contentOptions)
		{
			CBLRevisionInternal rev = new CBLRevisionInternal(docId, revId, false, this);
			rev.SetSequence(sequence);
			IDictionary<string, object> extra = ExtraPropertiesForRevision(rev, contentOptions
				);
			if (json == null)
			{
				return extra;
			}
			IDictionary<string, object> docProperties = null;
			try
			{
				docProperties = CBLServer.GetObjectMapper().ReadValue<IDictionary>(json);
				docProperties.PutAll(extra);
				return docProperties;
			}
			catch (Exception e)
			{
				Log.E(Couchbase.CBLDatabase.Tag, "Error serializing properties to JSON", e);
			}
			return docProperties;
		}

		public virtual CBLRevisionInternal GetDocumentWithIDAndRev(string id, string rev, 
			EnumSet<CBLDatabase.TDContentOptions> contentOptions)
		{
			CBLRevisionInternal result = null;
			string sql;
			Cursor cursor = null;
			try
			{
				cursor = null;
				string cols = "revid, deleted, sequence";
				if (!contentOptions.Contains(CBLDatabase.TDContentOptions.TDNoBody))
				{
					cols += ", json";
				}
				if (rev != null)
				{
					sql = "SELECT " + cols + " FROM revs, docs WHERE docs.docid=? AND revs.doc_id=docs.doc_id AND revid=? LIMIT 1";
					string[] args = new string[] { id, rev };
					cursor = sqliteDb.RawQuery(sql, args);
				}
				else
				{
					sql = "SELECT " + cols + " FROM revs, docs WHERE docs.docid=? AND revs.doc_id=docs.doc_id and current=1 and deleted=0 ORDER BY revid DESC LIMIT 1";
					string[] args = new string[] { id };
					cursor = sqliteDb.RawQuery(sql, args);
				}
				if (cursor.MoveToFirst())
				{
					if (rev == null)
					{
						rev = cursor.GetString(0);
					}
					bool deleted = (cursor.GetInt(1) > 0);
					result = new CBLRevisionInternal(id, rev, deleted, this);
					result.SetSequence(cursor.GetLong(2));
					if (!contentOptions.Equals(EnumSet.Of(CBLDatabase.TDContentOptions.TDNoBody)))
					{
						byte[] json = null;
						if (!contentOptions.Contains(CBLDatabase.TDContentOptions.TDNoBody))
						{
							json = cursor.GetBlob(3);
						}
						ExpandStoredJSONIntoRevisionWithAttachments(json, result, contentOptions);
					}
				}
			}
			catch (SQLException e)
			{
				Log.E(Couchbase.CBLDatabase.Tag, "Error getting document with id and rev", e);
			}
			finally
			{
				if (cursor != null)
				{
					cursor.Close();
				}
			}
			return result;
		}

		public virtual bool ExistsDocumentWithIDAndRev(string docId, string revId)
		{
			return GetDocumentWithIDAndRev(docId, revId, EnumSet.Of(CBLDatabase.TDContentOptions
				.TDNoBody)) != null;
		}

		/// <exception cref="Couchbase.CBLiteException"></exception>
		public virtual void LoadRevisionBody(CBLRevisionInternal rev, EnumSet<CBLDatabase.TDContentOptions
			> contentOptions)
		{
			if (rev.GetBody() != null)
			{
				return;
			}
			System.Diagnostics.Debug.Assert(((rev.GetDocId() != null) && (rev.GetRevId() != null
				)));
			Cursor cursor = null;
			CBLStatus result = new CBLStatus(CBLStatus.NotFound);
			try
			{
				string sql = "SELECT sequence, json FROM revs, docs WHERE revid=? AND docs.docid=? AND revs.doc_id=docs.doc_id LIMIT 1";
				string[] args = new string[] { rev.GetRevId(), rev.GetDocId() };
				cursor = sqliteDb.RawQuery(sql, args);
				if (cursor.MoveToFirst())
				{
					result.SetCode(CBLStatus.Ok);
					rev.SetSequence(cursor.GetLong(0));
					ExpandStoredJSONIntoRevisionWithAttachments(cursor.GetBlob(1), rev, contentOptions
						);
				}
			}
			catch (SQLException e)
			{
				Log.E(Couchbase.CBLDatabase.Tag, "Error loading revision body", e);
				throw new CBLiteException(CBLStatus.InternalServerError);
			}
			finally
			{
				if (cursor != null)
				{
					cursor.Close();
				}
			}
		}

		public virtual long GetDocNumericID(string docId)
		{
			Cursor cursor = null;
			string[] args = new string[] { docId };
			long result = -1;
			try
			{
				cursor = sqliteDb.RawQuery("SELECT doc_id FROM docs WHERE docid=?", args);
				if (cursor.MoveToFirst())
				{
					result = cursor.GetLong(0);
				}
				else
				{
					result = 0;
				}
			}
			catch (Exception e)
			{
				Log.E(Couchbase.CBLDatabase.Tag, "Error getting doc numeric id", e);
			}
			finally
			{
				if (cursor != null)
				{
					cursor.Close();
				}
			}
			return result;
		}

		/// <summary>Returns all the known revisions (or all current/conflicting revisions) of a document.
		/// 	</summary>
		/// <remarks>Returns all the known revisions (or all current/conflicting revisions) of a document.
		/// 	</remarks>
		public virtual CBLRevisionList GetAllRevisionsOfDocumentID(string docId, long docNumericID
			, bool onlyCurrent)
		{
			string sql = null;
			if (onlyCurrent)
			{
				sql = "SELECT sequence, revid, deleted FROM revs " + "WHERE doc_id=? AND current ORDER BY sequence DESC";
			}
			else
			{
				sql = "SELECT sequence, revid, deleted FROM revs " + "WHERE doc_id=? ORDER BY sequence DESC";
			}
			string[] args = new string[] { System.Convert.ToString(docNumericID) };
			Cursor cursor = null;
			cursor = sqliteDb.RawQuery(sql, args);
			CBLRevisionList result;
			try
			{
				cursor.MoveToFirst();
				result = new CBLRevisionList();
				while (!cursor.IsAfterLast())
				{
					CBLRevisionInternal rev = new CBLRevisionInternal(docId, cursor.GetString(1), (cursor
						.GetInt(2) > 0), this);
					rev.SetSequence(cursor.GetLong(0));
					result.AddItem(rev);
					cursor.MoveToNext();
				}
			}
			catch (SQLException e)
			{
				Log.E(Couchbase.CBLDatabase.Tag, "Error getting all revisions of document", e);
				return null;
			}
			finally
			{
				if (cursor != null)
				{
					cursor.Close();
				}
			}
			return result;
		}

		public virtual CBLRevisionList GetAllRevisionsOfDocumentID(string docId, bool onlyCurrent
			)
		{
			long docNumericId = GetDocNumericID(docId);
			if (docNumericId < 0)
			{
				return null;
			}
			else
			{
				if (docNumericId == 0)
				{
					return new CBLRevisionList();
				}
				else
				{
					return GetAllRevisionsOfDocumentID(docId, docNumericId, onlyCurrent);
				}
			}
		}

		public virtual IList<string> GetConflictingRevisionIDsOfDocID(string docID)
		{
			long docIdNumeric = GetDocNumericID(docID);
			if (docIdNumeric < 0)
			{
				return null;
			}
			IList<string> result = new AList<string>();
			Cursor cursor = null;
			try
			{
				string[] args = new string[] { System.Convert.ToString(docIdNumeric) };
				cursor = sqliteDb.RawQuery("SELECT revid FROM revs WHERE doc_id=? AND current " +
					 "ORDER BY revid DESC OFFSET 1", args);
				cursor.MoveToFirst();
				while (!cursor.IsAfterLast())
				{
					result.AddItem(cursor.GetString(0));
					cursor.MoveToNext();
				}
			}
			catch (SQLException e)
			{
				Log.E(Couchbase.CBLDatabase.Tag, "Error getting all revisions of document", e);
				return null;
			}
			finally
			{
				if (cursor != null)
				{
					cursor.Close();
				}
			}
			return result;
		}

		public virtual string FindCommonAncestorOf(CBLRevisionInternal rev, IList<string>
			 revIDs)
		{
			string result = null;
			if (revIDs.Count == 0)
			{
				return null;
			}
			string docId = rev.GetDocId();
			long docNumericID = GetDocNumericID(docId);
			if (docNumericID <= 0)
			{
				return null;
			}
			string quotedRevIds = JoinQuoted(revIDs);
			string sql = "SELECT revid FROM revs " + "WHERE doc_id=? and revid in (" + quotedRevIds
				 + ") and revid <= ? " + "ORDER BY revid DESC LIMIT 1";
			string[] args = new string[] { System.Convert.ToString(docNumericID) };
			Cursor cursor = null;
			try
			{
				cursor = sqliteDb.RawQuery(sql, args);
				cursor.MoveToFirst();
				if (!cursor.IsAfterLast())
				{
					result = cursor.GetString(0);
				}
			}
			catch (SQLException e)
			{
				Log.E(Couchbase.CBLDatabase.Tag, "Error getting all revisions of document", e);
			}
			finally
			{
				if (cursor != null)
				{
					cursor.Close();
				}
			}
			return result;
		}

		/// <summary>Returns an array of TDRevs in reverse chronological order, starting with the given revision.
		/// 	</summary>
		/// <remarks>Returns an array of TDRevs in reverse chronological order, starting with the given revision.
		/// 	</remarks>
		public virtual IList<CBLRevisionInternal> GetRevisionHistory(CBLRevisionInternal 
			rev)
		{
			string docId = rev.GetDocId();
			string revId = rev.GetRevId();
			System.Diagnostics.Debug.Assert(((docId != null) && (revId != null)));
			long docNumericId = GetDocNumericID(docId);
			if (docNumericId < 0)
			{
				return null;
			}
			else
			{
				if (docNumericId == 0)
				{
					return new AList<CBLRevisionInternal>();
				}
			}
			string sql = "SELECT sequence, parent, revid, deleted FROM revs " + "WHERE doc_id=? ORDER BY sequence DESC";
			string[] args = new string[] { System.Convert.ToString(docNumericId) };
			Cursor cursor = null;
			IList<CBLRevisionInternal> result;
			try
			{
				cursor = sqliteDb.RawQuery(sql, args);
				cursor.MoveToFirst();
				long lastSequence = 0;
				result = new AList<CBLRevisionInternal>();
				while (!cursor.IsAfterLast())
				{
					long sequence = cursor.GetLong(0);
					bool matches = false;
					if (lastSequence == 0)
					{
						matches = revId.Equals(cursor.GetString(2));
					}
					else
					{
						matches = (sequence == lastSequence);
					}
					if (matches)
					{
						revId = cursor.GetString(2);
						bool deleted = (cursor.GetInt(3) > 0);
						CBLRevisionInternal aRev = new CBLRevisionInternal(docId, revId, deleted, this);
						aRev.SetSequence(cursor.GetLong(0));
						result.AddItem(aRev);
						lastSequence = cursor.GetLong(1);
						if (lastSequence == 0)
						{
							break;
						}
					}
					cursor.MoveToNext();
				}
			}
			catch (SQLException e)
			{
				Log.E(Couchbase.CBLDatabase.Tag, "Error getting revision history", e);
				return null;
			}
			finally
			{
				if (cursor != null)
				{
					cursor.Close();
				}
			}
			return result;
		}

		// Splits a revision ID into its generation number and opaque suffix string
		public static int ParseRevIDNumber(string rev)
		{
			int result = -1;
			int dashPos = rev.IndexOf("-");
			if (dashPos >= 0)
			{
				try
				{
					result = System.Convert.ToInt32(Sharpen.Runtime.Substring(rev, 0, dashPos));
				}
				catch (FormatException)
				{
				}
			}
			// ignore, let it return -1
			return result;
		}

		// Splits a revision ID into its generation number and opaque suffix string
		public static string ParseRevIDSuffix(string rev)
		{
			string result = null;
			int dashPos = rev.IndexOf("-");
			if (dashPos >= 0)
			{
				result = Sharpen.Runtime.Substring(rev, dashPos + 1);
			}
			return result;
		}

		public static IDictionary<string, object> MakeRevisionHistoryDict(IList<CBLRevisionInternal
			> history)
		{
			if (history == null)
			{
				return null;
			}
			// Try to extract descending numeric prefixes:
			IList<string> suffixes = new AList<string>();
			int start = -1;
			int lastRevNo = -1;
			foreach (CBLRevisionInternal rev in history)
			{
				int revNo = ParseRevIDNumber(rev.GetRevId());
				string suffix = ParseRevIDSuffix(rev.GetRevId());
				if (revNo > 0 && suffix.Length > 0)
				{
					if (start < 0)
					{
						start = revNo;
					}
					else
					{
						if (revNo != lastRevNo - 1)
						{
							start = -1;
							break;
						}
					}
					lastRevNo = revNo;
					suffixes.AddItem(suffix);
				}
				else
				{
					start = -1;
					break;
				}
			}
			IDictionary<string, object> result = new Dictionary<string, object>();
			if (start == -1)
			{
				// we failed to build sequence, just stuff all the revs in list
				suffixes = new AList<string>();
				foreach (CBLRevisionInternal rev_1 in history)
				{
					suffixes.AddItem(rev_1.GetRevId());
				}
			}
			else
			{
				result.Put("start", start);
			}
			result.Put("ids", suffixes);
			return result;
		}

		/// <summary>Returns the revision history as a _revisions dictionary, as returned by the REST API's ?revs=true option.
		/// 	</summary>
		/// <remarks>Returns the revision history as a _revisions dictionary, as returned by the REST API's ?revs=true option.
		/// 	</remarks>
		public virtual IDictionary<string, object> GetRevisionHistoryDict(CBLRevisionInternal
			 rev)
		{
			return MakeRevisionHistoryDict(GetRevisionHistory(rev));
		}

		public virtual CBLRevisionList ChangesSince(long lastSeq, CBLChangesOptions options
			, CBLFilterBlock filter)
		{
			// http://wiki.apache.org/couchdb/HTTP_database_API#Changes
			if (options == null)
			{
				options = new CBLChangesOptions();
			}
			bool includeDocs = options.IsIncludeDocs() || (filter != null);
			string additionalSelectColumns = string.Empty;
			if (includeDocs)
			{
				additionalSelectColumns = ", json";
			}
			string sql = "SELECT sequence, revs.doc_id, docid, revid, deleted" + additionalSelectColumns
				 + " FROM revs, docs " + "WHERE sequence > ? AND current=1 " + "AND revs.doc_id = docs.doc_id "
				 + "ORDER BY revs.doc_id, revid DESC";
			string[] args = new string[] { System.Convert.ToString(lastSeq) };
			Cursor cursor = null;
			CBLRevisionList changes = null;
			try
			{
				cursor = sqliteDb.RawQuery(sql, args);
				cursor.MoveToFirst();
				changes = new CBLRevisionList();
				long lastDocId = 0;
				while (!cursor.IsAfterLast())
				{
					if (!options.IsIncludeConflicts())
					{
						// Only count the first rev for a given doc (the rest will be losing conflicts):
						long docNumericId = cursor.GetLong(1);
						if (docNumericId == lastDocId)
						{
							cursor.MoveToNext();
							continue;
						}
						lastDocId = docNumericId;
					}
					CBLRevisionInternal rev = new CBLRevisionInternal(cursor.GetString(2), cursor.GetString
						(3), (cursor.GetInt(4) > 0), this);
					rev.SetSequence(cursor.GetLong(0));
					if (includeDocs)
					{
						ExpandStoredJSONIntoRevisionWithAttachments(cursor.GetBlob(5), rev, options.GetContentOptions
							());
					}
					if ((filter == null) || (filter.Filter(rev)))
					{
						changes.AddItem(rev);
					}
					cursor.MoveToNext();
				}
			}
			catch (SQLException e)
			{
				Log.E(Couchbase.CBLDatabase.Tag, "Error looking for changes", e);
			}
			finally
			{
				if (cursor != null)
				{
					cursor.Close();
				}
			}
			if (options.IsSortBySequence())
			{
				changes.SortBySequence();
			}
			changes.Limit(options.GetLimit());
			return changes;
		}

		/// <summary>Define or clear a named filter function.</summary>
		/// <remarks>
		/// Define or clear a named filter function.
		/// Filters are used by push replications to choose which documents to send.
		/// TODO: is the comment below still valid?  It's not in the iOS docs.
		/// These aren't used directly by CBLDatabase, but they're looked up by CBLRouter
		/// when a _changes request has a ?filter parameter.
		/// </remarks>
		public virtual void DefineFilter(string filterName, CBLFilterBlock filter)
		{
			if (filters == null)
			{
				filters = new Dictionary<string, CBLFilterBlock>();
			}
			if (filter != null)
			{
				filters.Put(filterName, filter);
			}
			else
			{
				Sharpen.Collections.Remove(filters, filterName);
			}
		}

		/// <summary>Returns the existing filter function (block) registered with the given name.
		/// 	</summary>
		/// <remarks>
		/// Returns the existing filter function (block) registered with the given name.
		/// Note that filters are not persistent -- you have to re-register them on every launch.
		/// </remarks>
		public virtual CBLFilterBlock GetFilter(string filterName)
		{
			CBLFilterBlock result = null;
			if (filters != null)
			{
				result = filters.Get(filterName);
			}
			return result;
		}

		/// <summary>VIEWS:</summary>
		public virtual CBLView RegisterView(CBLView view)
		{
			if (view == null)
			{
				return null;
			}
			if (views == null)
			{
				views = new Dictionary<string, CBLView>();
			}
			views.Put(view.GetName(), view);
			return view;
		}

		/// <summary>Returns a CBLView object for the view with the given name.</summary>
		/// <remarks>
		/// Returns a CBLView object for the view with the given name.
		/// (This succeeds even if the view doesn't already exist, but the view won't be added to
		/// the database until the CBLView is assigned a map function.)
		/// </remarks>
		public virtual CBLView GetView(string name)
		{
			CBLView view = null;
			if (views != null)
			{
				view = views.Get(name);
			}
			if (view != null)
			{
				return view;
			}
			return RegisterView(new CBLView(this, name));
		}

		/// <exception cref="Couchbase.CBLiteException"></exception>
		public virtual IList<CBLQueryRow> QueryViewNamed(string viewName, CBLQueryOptions
			 options, IList<long> outLastSequence)
		{
			long lastSequence = 0;
			IList<CBLQueryRow> rows = null;
			if (viewName != null && viewName.Length > 0)
			{
				CBLView view = GetView(viewName);
				if (view == null)
				{
					throw new CBLiteException(new CBLStatus(CBLStatus.NotFound));
				}
				lastSequence = view.GetLastSequenceIndexed();
				if (options.GetStale() == CBLQuery.CBLStaleness.CBLStaleNever || lastSequence <= 
					0)
				{
					view.UpdateIndex();
					lastSequence = view.GetLastSequenceIndexed();
				}
				else
				{
					if (options.GetStale() == CBLQuery.CBLStaleness.CBLStaleUpdateAfter && lastSequence
						 < GetLastSequence())
					{
						new Sharpen.Thread(new _Runnable_1301(view)).Start();
					}
				}
				rows = view.QueryWithOptions(options);
			}
			else
			{
				// nil view means query _all_docs
				// note: this is a little kludgy, but we have to pull out the "rows" field from the
				// result dictionary because that's what we want.  should be refactored, but
				// it's a little tricky, so postponing.
				IDictionary<string, object> allDocsResult = GetAllDocs(options);
				rows = (IList<CBLQueryRow>)allDocsResult.Get("rows");
				lastSequence = GetLastSequence();
			}
			outLastSequence.AddItem(lastSequence);
			return rows;
		}

		private sealed class _Runnable_1301 : Runnable
		{
			public _Runnable_1301(CBLView view)
			{
				this.view = view;
			}

			public void Run()
			{
				try
				{
					view.UpdateIndex();
				}
				catch (CBLiteException e)
				{
					Log.E(Couchbase.CBLDatabase.Tag, "Error updating view index on background thread"
						, e);
				}
			}

			private readonly CBLView view;
		}

		internal virtual CBLView MakeAnonymousView()
		{
			for (int i = 0; true; ++i)
			{
				string name = string.Format("anon%d", i);
				CBLView existing = GetExistingView(name);
				if (existing == null)
				{
					// this name has not been used yet, so let's use it
					return GetView(name);
				}
			}
		}

		/// <summary>Returns the existing CBLView with the given name, or nil if none.</summary>
		/// <remarks>Returns the existing CBLView with the given name, or nil if none.</remarks>
		public virtual CBLView GetExistingView(string name)
		{
			CBLView view = null;
			if (views != null)
			{
				view = views.Get(name);
			}
			if (view != null)
			{
				return view;
			}
			view = new CBLView(this, name);
			if (view.GetViewId() == 0)
			{
				return null;
			}
			return RegisterView(view);
		}

		public virtual IList<CBLView> GetAllViews()
		{
			Cursor cursor = null;
			IList<CBLView> result = null;
			try
			{
				cursor = sqliteDb.RawQuery("SELECT name FROM views", null);
				cursor.MoveToFirst();
				result = new AList<CBLView>();
				while (!cursor.IsAfterLast())
				{
					result.AddItem(GetView(cursor.GetString(0)));
					cursor.MoveToNext();
				}
			}
			catch (Exception e)
			{
				Log.E(Couchbase.CBLDatabase.Tag, "Error getting all views", e);
			}
			finally
			{
				if (cursor != null)
				{
					cursor.Close();
				}
			}
			return result;
		}

		public virtual CBLStatus DeleteViewNamed(string name)
		{
			CBLStatus result = new CBLStatus(CBLStatus.InternalServerError);
			try
			{
				string[] whereArgs = new string[] { name };
				int rowsAffected = sqliteDb.Delete("views", "name=?", whereArgs);
				if (rowsAffected > 0)
				{
					result.SetCode(CBLStatus.Ok);
				}
				else
				{
					result.SetCode(CBLStatus.NotFound);
				}
			}
			catch (SQLException e)
			{
				Log.E(Couchbase.CBLDatabase.Tag, "Error deleting view", e);
			}
			return result;
		}

		/// <exception cref="Couchbase.CBLiteException"></exception>
		public virtual IDictionary<string, object> GetAllDocs(CBLQueryOptions options)
		{
			IDictionary<string, object> result = new Dictionary<string, object>();
			IList<CBLQueryRow> rows = new AList<CBLQueryRow>();
			if (options == null)
			{
				options = new CBLQueryOptions();
			}
			long updateSeq = 0;
			if (options.IsUpdateSeq())
			{
				updateSeq = GetLastSequence();
			}
			// TODO: needs to be atomic with the following SELECT
			StringBuilder sql = new StringBuilder("SELECT revs.doc_id, docid, revid, sequence"
				);
			if (options.IsIncludeDocs())
			{
				sql.Append(", json");
			}
			if (options.IsIncludeDeletedDocs())
			{
				sql.Append(", deleted");
			}
			sql.Append(" FROM revs, docs WHERE");
			if (options.GetKeys() != null)
			{
				if (options.GetKeys().Count == 0)
				{
					return result;
				}
				string commaSeperatedIds = JoinQuotedObjects(options.GetKeys());
				sql.Append(string.Format(" revs.doc_id IN (SELECT doc_id FROM docs WHERE docid IN (%@)) AND"
					, commaSeperatedIds));
			}
			sql.Append(" docs.doc_id = revs.doc_id AND current=1");
			if (!options.IsIncludeDeletedDocs())
			{
				sql.Append(" AND deleted=0");
			}
			IList<string> args = new AList<string>();
			object minKey = options.GetStartKey();
			object maxKey = options.GetEndKey();
			bool inclusiveMin = true;
			bool inclusiveMax = options.IsInclusiveEnd();
			if (options.IsDescending())
			{
				minKey = maxKey;
				maxKey = options.GetStartKey();
				inclusiveMin = inclusiveMax;
				inclusiveMax = true;
			}
			if (minKey != null)
			{
				System.Diagnostics.Debug.Assert((minKey is string));
				sql.Append((inclusiveMin ? " AND docid >= ?" : " AND docid > ?"));
				args.AddItem((string)minKey);
			}
			if (maxKey != null)
			{
				System.Diagnostics.Debug.Assert((maxKey is string));
				sql.Append((inclusiveMax ? " AND docid <= ?" : " AND docid < ?"));
				args.AddItem((string)maxKey);
			}
			sql.Append(string.Format(" ORDER BY docid %s, %s revid DESC LIMIT ? OFFSET ?", (options
				.IsDescending() ? "DESC" : "ASC"), (options.IsIncludeDeletedDocs() ? "deleted ASC,"
				 : string.Empty)));
			args.AddItem(Sharpen.Extensions.ToString(options.GetLimit()));
			args.AddItem(Sharpen.Extensions.ToString(options.GetSkip()));
			Cursor cursor = null;
			long lastDocID = 0;
			IDictionary<string, CBLQueryRow> docs = new Dictionary<string, CBLQueryRow>();
			try
			{
				cursor = sqliteDb.RawQuery(sql.ToString(), Sharpen.Collections.ToArray(args, new 
					string[args.Count]));
				cursor.MoveToFirst();
				while (!cursor.IsAfterLast())
				{
					long docNumericID = cursor.GetLong(0);
					if (docNumericID == lastDocID)
					{
						cursor.MoveToNext();
						continue;
					}
					lastDocID = docNumericID;
					string docId = cursor.GetString(1);
					string revId = cursor.GetString(2);
					long sequenceNumber = cursor.GetLong(3);
					bool deleted = options.IsIncludeDeletedDocs() && cursor.GetInt(cursor.GetColumnIndex
						("deleted")) > 0;
					IDictionary<string, object> docContents = null;
					if (options.IsIncludeDocs())
					{
						byte[] json = cursor.GetBlob(4);
						docContents = DocumentPropertiesFromJSON(json, docId, revId, sequenceNumber, options
							.GetContentOptions());
					}
					IDictionary<string, object> value = new Dictionary<string, object>();
					value.Put("rev", revId);
					value.Put("deleted", (deleted ? true : null));
					CBLQueryRow change = new CBLQueryRow(docId, sequenceNumber, docId, value, docContents
						);
					if (options.GetKeys() != null)
					{
						docs.Put(docId, change);
					}
					else
					{
						rows.AddItem(change);
					}
					cursor.MoveToNext();
				}
				if (options.GetKeys() != null)
				{
					foreach (object docIdObject in options.GetKeys())
					{
						if (docIdObject is string)
						{
							string docId = (string)docIdObject;
							CBLQueryRow change = docs.Get(docId);
							if (change == null)
							{
								IDictionary<string, object> value = new Dictionary<string, object>();
								long docNumericID = GetDocNumericID(docId);
								if (docNumericID > 0)
								{
									bool deleted;
									IList<bool> outIsDeleted = new AList<bool>();
									IList<bool> outIsConflict = new AList<bool>();
									string revId = WinningRevIDOfDoc(docNumericID, outIsDeleted, outIsConflict);
									if (outIsDeleted.Count > 0)
									{
										deleted = true;
									}
									if (revId != null)
									{
										value.Put("rev", revId);
										value.Put("deleted", true);
									}
								}
								change = new CBLQueryRow((value != null ? docId : null), 0, docId, value, null);
							}
							rows.AddItem(change);
						}
					}
				}
			}
			catch (SQLException e)
			{
				Log.E(Couchbase.CBLDatabase.Tag, "Error getting all docs", e);
				throw new CBLiteException("Error getting all docs", e, new CBLStatus(CBLStatus.InternalServerError
					));
			}
			finally
			{
				if (cursor != null)
				{
					cursor.Close();
				}
			}
			int totalRows = cursor.GetCount();
			//??? Is this true, or does it ignore limit/offset?
			result.Put("rows", rows);
			result.Put("total_rows", totalRows);
			result.Put("offset", options.GetSkip());
			if (updateSeq != 0)
			{
				result.Put("update_seq", updateSeq);
			}
			return result;
		}

		/// <summary>Returns the rev ID of the 'winning' revision of this document, and whether it's deleted.
		/// 	</summary>
		/// <remarks>Returns the rev ID of the 'winning' revision of this document, and whether it's deleted.
		/// 	</remarks>
		/// <exception cref="Couchbase.CBLiteException"></exception>
		internal virtual string WinningRevIDOfDoc(long docNumericId, IList<bool> outIsDeleted
			, IList<bool> outIsConflict)
		{
			Cursor cursor = null;
			string sql = "SELECT revid, deleted FROM revs" + " WHERE doc_id=? and current=1" 
				+ " ORDER BY deleted asc, revid desc LIMIT 2";
			string[] args = new string[] { System.Convert.ToString(docNumericId) };
			string revId = null;
			try
			{
				cursor = sqliteDb.RawQuery(sql, args);
				cursor.MoveToFirst();
				if (!cursor.IsAfterLast())
				{
					revId = cursor.GetString(0);
					bool deleted = cursor.GetInt(1) > 0;
					if (deleted)
					{
						outIsDeleted.AddItem(true);
					}
					// The document is in conflict if there are two+ result rows that are not deletions.
					bool hasNextResult = cursor.MoveToNext();
					bool isNextDeleted = cursor.GetInt(1) > 0;
					bool isInConflict = !deleted && hasNextResult && isNextDeleted;
					if (isInConflict)
					{
						outIsConflict.AddItem(true);
					}
				}
			}
			catch (SQLException e)
			{
				Log.E(Couchbase.CBLDatabase.Tag, "Error", e);
				throw new CBLiteException("Error", e, new CBLStatus(CBLStatus.InternalServerError
					));
			}
			finally
			{
				if (cursor != null)
				{
					cursor.Close();
				}
			}
			return revId;
		}

		/// <summary>OLD DEPRECATED UNUSED</summary>
		/// <param name="docIDs"></param>
		/// <param name="options"></param>
		/// <returns></returns>
		public virtual IDictionary<string, object> GetDocsWithIDs(IList<string> docIDs, CBLQueryOptions
			 options)
		{
			//FIX: This has a lot of code in common with -[CBLView queryWithOptions:status:]. Unify the two!
			if (options == null)
			{
				options = new CBLQueryOptions();
			}
			long updateSeq = 0;
			if (options.IsUpdateSeq())
			{
				updateSeq = GetLastSequence();
			}
			// TODO: needs to be atomic with the following SELECT
			// Generate the SELECT statement, based on the options:
			string additionalCols = string.Empty;
			if (options.IsIncludeDocs())
			{
				additionalCols = ", json, sequence";
			}
			string sql = "SELECT revs.doc_id, docid, revid, deleted" + additionalCols + " FROM revs, docs WHERE";
			if (docIDs != null)
			{
				sql += " docid IN (" + JoinQuoted(docIDs) + ")";
			}
			else
			{
				sql += " deleted=0";
			}
			sql += " AND current=1 AND docs.doc_id = revs.doc_id";
			IList<string> argsList = new AList<string>();
			object minKey = options.GetStartKey();
			object maxKey = options.GetEndKey();
			bool inclusiveMin = true;
			bool inclusiveMax = options.IsInclusiveEnd();
			if (options.IsDescending())
			{
				minKey = maxKey;
				maxKey = options.GetStartKey();
				inclusiveMin = inclusiveMax;
				inclusiveMax = true;
			}
			if (minKey != null)
			{
				System.Diagnostics.Debug.Assert((minKey is string));
				if (inclusiveMin)
				{
					sql += " AND docid >= ?";
				}
				else
				{
					sql += " AND docid > ?";
				}
				argsList.AddItem((string)minKey);
			}
			if (maxKey != null)
			{
				System.Diagnostics.Debug.Assert((maxKey is string));
				if (inclusiveMax)
				{
					sql += " AND docid <= ?";
				}
				else
				{
					sql += " AND docid < ?";
				}
				argsList.AddItem((string)maxKey);
			}
			string order = "ASC";
			if (options.IsDescending())
			{
				order = "DESC";
			}
			sql += " ORDER BY docid " + order + ", revid DESC LIMIT ? OFFSET ?";
			argsList.AddItem(Sharpen.Extensions.ToString(options.GetLimit()));
			argsList.AddItem(Sharpen.Extensions.ToString(options.GetSkip()));
			Cursor cursor = null;
			long lastDocID = 0;
			IList<IDictionary<string, object>> rows = null;
			try
			{
				cursor = sqliteDb.RawQuery(sql, Sharpen.Collections.ToArray(argsList, new string[
					argsList.Count]));
				cursor.MoveToFirst();
				rows = new AList<IDictionary<string, object>>();
				while (!cursor.IsAfterLast())
				{
					long docNumericID = cursor.GetLong(0);
					if (docNumericID == lastDocID)
					{
						cursor.MoveToNext();
						continue;
					}
					lastDocID = docNumericID;
					string docId = cursor.GetString(1);
					string revId = cursor.GetString(2);
					IDictionary<string, object> docContents = null;
					bool deleted = cursor.GetInt(3) > 0;
					if (options.IsIncludeDocs() && !deleted)
					{
						byte[] json = cursor.GetBlob(4);
						long sequence = cursor.GetLong(5);
						docContents = DocumentPropertiesFromJSON(json, docId, revId, sequence, options.GetContentOptions
							());
					}
					IDictionary<string, object> valueMap = new Dictionary<string, object>();
					valueMap.Put("rev", revId);
					IDictionary<string, object> change = new Dictionary<string, object>();
					change.Put("id", docId);
					change.Put("key", docId);
					change.Put("value", valueMap);
					if (docContents != null)
					{
						change.Put("doc", docContents);
					}
					if (deleted)
					{
						change.Put("deleted", true);
					}
					rows.AddItem(change);
					cursor.MoveToNext();
				}
			}
			catch (SQLException e)
			{
				Log.E(Couchbase.CBLDatabase.Tag, "Error getting all docs", e);
				return null;
			}
			finally
			{
				if (cursor != null)
				{
					cursor.Close();
				}
			}
			int totalRows = cursor.GetCount();
			//??? Is this true, or does it ignore limit/offset?
			IDictionary<string, object> result = new Dictionary<string, object>();
			result.Put("rows", rows);
			result.Put("total_rows", totalRows);
			result.Put("offset", options.GetSkip());
			if (updateSeq != 0)
			{
				result.Put("update_seq", updateSeq);
			}
			return result;
		}

		public virtual void AddChangeListener(CBLDatabaseChangedFunction listener)
		{
			// TODO: implement!
			throw new RuntimeException("Not implemented");
		}

		public virtual void RemoveChangeListener(CBLDatabaseChangedFunction listener)
		{
			// TODO: implement!
			throw new RuntimeException("Not implemented");
		}

		/// <exception cref="Couchbase.CBLiteException"></exception>
		internal virtual void InsertAttachmentForSequence(CBLAttachmentInternal attachment
			, long sequence)
		{
			InsertAttachmentForSequenceWithNameAndType(sequence, attachment.GetName(), attachment
				.GetContentType(), attachment.GetRevpos(), attachment.GetBlobKey());
		}

		/// <exception cref="Couchbase.CBLiteException"></exception>
		public virtual void InsertAttachmentForSequenceWithNameAndType(InputStream contentStream
			, long sequence, string name, string contentType, int revpos)
		{
			System.Diagnostics.Debug.Assert((sequence > 0));
			System.Diagnostics.Debug.Assert((name != null));
			CBLBlobKey key = new CBLBlobKey();
			if (!attachments.StoreBlobStream(contentStream, key))
			{
				throw new CBLiteException(CBLStatus.InternalServerError);
			}
			InsertAttachmentForSequenceWithNameAndType(sequence, name, contentType, revpos, key
				);
		}

		/// <exception cref="Couchbase.CBLiteException"></exception>
		public virtual void InsertAttachmentForSequenceWithNameAndType(long sequence, string
			 name, string contentType, int revpos, CBLBlobKey key)
		{
			try
			{
				ContentValues args = new ContentValues();
				args.Put("sequence", sequence);
				args.Put("filename", name);
				args.Put("key", key.GetBytes());
				args.Put("type", contentType);
				args.Put("length", attachments.GetSizeOfBlob(key));
				args.Put("revpos", revpos);
				sqliteDb.Insert("attachments", null, args);
			}
			catch (SQLException e)
			{
				Log.E(Couchbase.CBLDatabase.Tag, "Error inserting attachment", e);
				throw new CBLiteException(CBLStatus.InternalServerError);
			}
		}

		/// <exception cref="Couchbase.CBLiteException"></exception>
		internal virtual void InstallAttachment(CBLAttachmentInternal attachment, IDictionary
			<string, object> attachInfo)
		{
			string digest = (string)attachInfo.Get("digest");
			if (digest == null)
			{
				throw new CBLiteException(CBLStatus.BadAttachment);
			}
			if (pendingAttachmentsByDigest != null && pendingAttachmentsByDigest.ContainsKey(
				digest))
			{
				CBLBlobStoreWriter writer = pendingAttachmentsByDigest.Get(digest);
				try
				{
					CBLBlobStoreWriter blobStoreWriter = (CBLBlobStoreWriter)writer;
					blobStoreWriter.Install();
					attachment.SetBlobKey(blobStoreWriter.GetBlobKey());
					attachment.SetLength(blobStoreWriter.GetLength());
				}
				catch (Exception e)
				{
					throw new CBLiteException(e, CBLStatus.StatusAttachmentError);
				}
			}
		}

		private IDictionary<string, CBLBlobStoreWriter> GetPendingAttachmentsByDigest()
		{
			if (pendingAttachmentsByDigest == null)
			{
				pendingAttachmentsByDigest = new Dictionary<string, CBLBlobStoreWriter>();
			}
			return pendingAttachmentsByDigest;
		}

		/// <exception cref="Couchbase.CBLiteException"></exception>
		public virtual void CopyAttachmentNamedFromSequenceToSequence(string name, long fromSeq
			, long toSeq)
		{
			System.Diagnostics.Debug.Assert((name != null));
			System.Diagnostics.Debug.Assert((toSeq > 0));
			if (fromSeq < 0)
			{
				throw new CBLiteException(CBLStatus.NotFound);
			}
			Cursor cursor = null;
			string[] args = new string[] { System.Convert.ToString(toSeq), name, System.Convert.ToString
				(fromSeq), name };
			try
			{
				sqliteDb.ExecSQL("INSERT INTO attachments (sequence, filename, key, type, length, revpos) "
					 + "SELECT ?, ?, key, type, length, revpos FROM attachments " + "WHERE sequence=? AND filename=?"
					, args);
				cursor = sqliteDb.RawQuery("SELECT changes()", null);
				cursor.MoveToFirst();
				int rowsUpdated = cursor.GetInt(0);
				if (rowsUpdated == 0)
				{
					// Oops. This means a glitch in our attachment-management or pull code,
					// or else a bug in the upstream server.
					Log.W(Couchbase.CBLDatabase.Tag, "Can't find inherited attachment " + name + " from seq# "
						 + System.Convert.ToString(fromSeq) + " to copy to " + System.Convert.ToString(toSeq
						));
					throw new CBLiteException(CBLStatus.NotFound);
				}
				else
				{
					return;
				}
			}
			catch (SQLException e)
			{
				Log.E(Couchbase.CBLDatabase.Tag, "Error copying attachment", e);
				throw new CBLiteException(CBLStatus.InternalServerError);
			}
			finally
			{
				if (cursor != null)
				{
					cursor.Close();
				}
			}
		}

		/// <summary>Returns the content and MIME type of an attachment</summary>
		/// <exception cref="Couchbase.CBLiteException"></exception>
		public virtual CBLAttachment GetAttachmentForSequence(long sequence, string filename
			)
		{
			System.Diagnostics.Debug.Assert((sequence > 0));
			System.Diagnostics.Debug.Assert((filename != null));
			Cursor cursor = null;
			string[] args = new string[] { System.Convert.ToString(sequence), filename };
			try
			{
				cursor = sqliteDb.RawQuery("SELECT key, type FROM attachments WHERE sequence=? AND filename=?"
					, args);
				if (!cursor.MoveToFirst())
				{
					throw new CBLiteException(CBLStatus.NotFound);
				}
				byte[] keyData = cursor.GetBlob(0);
				//TODO add checks on key here? (ios version)
				CBLBlobKey key = new CBLBlobKey(keyData);
				InputStream contentStream = attachments.BlobStreamForKey(key);
				if (contentStream == null)
				{
					Log.E(Couchbase.CBLDatabase.Tag, "Failed to load attachment");
					throw new CBLiteException(CBLStatus.InternalServerError);
				}
				else
				{
					CBLAttachment result = new CBLAttachment(contentStream, cursor.GetString(1));
					return result;
				}
			}
			catch (SQLException)
			{
				throw new CBLiteException(CBLStatus.InternalServerError);
			}
			finally
			{
				if (cursor != null)
				{
					cursor.Close();
				}
			}
		}

		/// <summary>Returns the location of an attachment's file in the blob store.</summary>
		/// <remarks>Returns the location of an attachment's file in the blob store.</remarks>
		/// <exception cref="Couchbase.CBLiteException"></exception>
		internal virtual string GetAttachmentPathForSequence(long sequence, string filename
			)
		{
			System.Diagnostics.Debug.Assert((sequence > 0));
			System.Diagnostics.Debug.Assert((filename != null));
			Cursor cursor = null;
			string filePath = null;
			string[] args = new string[] { System.Convert.ToString(sequence), filename };
			try
			{
				cursor = sqliteDb.RawQuery("SELECT key, type, encoding FROM attachments WHERE sequence=? AND filename=?"
					, args);
				if (!cursor.MoveToFirst())
				{
					throw new CBLiteException(CBLStatus.NotFound);
				}
				byte[] keyData = cursor.GetBlob(0);
				CBLBlobKey key = new CBLBlobKey(keyData);
				filePath = GetAttachments().PathForKey(key);
				return filePath;
			}
			catch (SQLException)
			{
				throw new CBLiteException(CBLStatus.InternalServerError);
			}
			finally
			{
				if (cursor != null)
				{
					cursor.Close();
				}
			}
		}

		/// <summary>Constructs an "_attachments" dictionary for a revision, to be inserted in its JSON body.
		/// 	</summary>
		/// <remarks>Constructs an "_attachments" dictionary for a revision, to be inserted in its JSON body.
		/// 	</remarks>
		public virtual IDictionary<string, object> GetAttachmentsDictForSequenceWithContent
			(long sequence, EnumSet<CBLDatabase.TDContentOptions> contentOptions)
		{
			System.Diagnostics.Debug.Assert((sequence > 0));
			Cursor cursor = null;
			string[] args = new string[] { System.Convert.ToString(sequence) };
			try
			{
				cursor = sqliteDb.RawQuery("SELECT filename, key, type, length, revpos FROM attachments WHERE sequence=?"
					, args);
				if (!cursor.MoveToFirst())
				{
					return null;
				}
				IDictionary<string, object> result = new Dictionary<string, object>();
				while (!cursor.IsAfterLast())
				{
					bool dataSuppressed = false;
					int length = cursor.GetInt(3);
					byte[] keyData = cursor.GetBlob(1);
					CBLBlobKey key = new CBLBlobKey(keyData);
					string digestString = "sha1-" + Base64.EncodeBytes(keyData);
					string dataBase64 = null;
					if (contentOptions.Contains(CBLDatabase.TDContentOptions.TDIncludeAttachments))
					{
						if (contentOptions.Contains(CBLDatabase.TDContentOptions.TDBigAttachmentsFollow) 
							&& length >= Couchbase.CBLDatabase.kBigAttachmentLength)
						{
							dataSuppressed = true;
						}
						else
						{
							byte[] data = attachments.BlobForKey(key);
							if (data != null)
							{
								dataBase64 = Base64.EncodeBytes(data);
							}
							else
							{
								// <-- very expensive
								Log.W(Couchbase.CBLDatabase.Tag, "Error loading attachment");
							}
						}
					}
					IDictionary<string, object> attachment = new Dictionary<string, object>();
					if (dataBase64 == null || dataSuppressed == true)
					{
						attachment.Put("stub", true);
					}
					if (dataBase64 != null)
					{
						attachment.Put("data", dataBase64);
					}
					if (dataSuppressed == true)
					{
						attachment.Put("follows", true);
					}
					attachment.Put("digest", digestString);
					string contentType = cursor.GetString(2);
					attachment.Put("content_type", contentType);
					attachment.Put("length", length);
					attachment.Put("revpos", cursor.GetInt(4));
					string filename = cursor.GetString(0);
					result.Put(filename, attachment);
					cursor.MoveToNext();
				}
				return result;
			}
			catch (SQLException e)
			{
				Log.E(Couchbase.CBLDatabase.Tag, "Error getting attachments for sequence", e);
				return null;
			}
			finally
			{
				if (cursor != null)
				{
					cursor.Close();
				}
			}
		}

		/// <summary>Modifies a CBLRevisionInternal's body by changing all attachments with revpos &lt; minRevPos into stubs.
		/// 	</summary>
		/// <remarks>Modifies a CBLRevisionInternal's body by changing all attachments with revpos &lt; minRevPos into stubs.
		/// 	</remarks>
		/// <param name="rev"></param>
		/// <param name="minRevPos"></param>
		public virtual void StubOutAttachmentsIn(CBLRevisionInternal rev, int minRevPos)
		{
			if (minRevPos <= 1)
			{
				return;
			}
			IDictionary<string, object> properties = (IDictionary<string, object>)rev.GetProperties
				();
			IDictionary<string, object> attachments = null;
			if (properties != null)
			{
				attachments = (IDictionary<string, object>)properties.Get("_attachments");
			}
			IDictionary<string, object> editedProperties = null;
			IDictionary<string, object> editedAttachments = null;
			foreach (string name in attachments.Keys)
			{
				IDictionary<string, object> attachment = (IDictionary<string, object>)attachments
					.Get(name);
				int revPos = (int)attachment.Get("revpos");
				object stub = attachment.Get("stub");
				if (revPos > 0 && revPos < minRevPos && (stub == null))
				{
					// Strip this attachment's body. First make its dictionary mutable:
					if (editedProperties == null)
					{
						editedProperties = new Dictionary<string, object>(properties);
						editedAttachments = new Dictionary<string, object>(attachments);
						editedProperties.Put("_attachments", editedAttachments);
					}
					// ...then remove the 'data' and 'follows' key:
					IDictionary<string, object> editedAttachment = new Dictionary<string, object>(attachment
						);
					Sharpen.Collections.Remove(editedAttachment, "data");
					Sharpen.Collections.Remove(editedAttachment, "follows");
					editedAttachment.Put("stub", true);
					editedAttachments.Put(name, editedAttachment);
					Log.D(Couchbase.CBLDatabase.Tag, "Stubbed out attachment" + rev + " " + name + ": revpos"
						 + revPos + " " + minRevPos);
				}
			}
			if (editedProperties != null)
			{
				rev.SetProperties(editedProperties);
			}
		}

		internal virtual void StubOutAttachmentsInRevision(IDictionary<string, CBLAttachmentInternal
			> attachments, CBLRevisionInternal rev)
		{
			IDictionary<string, object> properties = rev.GetProperties();
			IDictionary<string, object> attachmentsFromProps = (IDictionary<string, object>)properties
				.Get("_attachments");
			foreach (string attachmentKey in attachmentsFromProps.Keys)
			{
				IDictionary<string, object> attachmentFromProps = (IDictionary<string, object>)attachmentsFromProps
					.Get(attachmentKey);
				if (attachmentFromProps.Get("follows") != null || attachmentFromProps.Get("data")
					 != null)
				{
					Sharpen.Collections.Remove(attachmentFromProps, "follows");
					Sharpen.Collections.Remove(attachmentFromProps, "data");
					attachmentFromProps.Put("stub", true);
					if (attachmentFromProps.Get("revpos") == null)
					{
						attachmentFromProps.Put("revpos", rev.GetGeneration());
					}
					CBLAttachmentInternal attachmentObject = attachments.Get(attachmentKey);
					if (attachmentObject != null)
					{
						attachmentFromProps.Put("length", attachmentObject.GetLength());
						attachmentFromProps.Put("digest", attachmentObject.GetBlobKey().Base64Digest());
					}
					attachmentFromProps.Put(attachmentKey, attachmentFromProps);
				}
			}
		}

		/// <summary>
		/// Given a newly-added revision, adds the necessary attachment rows to the sqliteDb and
		/// stores inline attachments into the blob store.
		/// </summary>
		/// <remarks>
		/// Given a newly-added revision, adds the necessary attachment rows to the sqliteDb and
		/// stores inline attachments into the blob store.
		/// </remarks>
		/// <exception cref="Couchbase.CBLiteException"></exception>
		internal virtual void ProcessAttachmentsForRevision(IDictionary<string, CBLAttachmentInternal
			> attachments, CBLRevisionInternal rev, long parentSequence)
		{
			System.Diagnostics.Debug.Assert((rev != null));
			long newSequence = rev.GetSequence();
			System.Diagnostics.Debug.Assert((newSequence > parentSequence));
			int generation = rev.GetGeneration();
			System.Diagnostics.Debug.Assert((generation > 0));
			// If there are no attachments in the new rev, there's nothing to do:
			IDictionary<string, object> revAttachments = null;
			IDictionary<string, object> properties = (IDictionary<string, object>)rev.GetProperties
				();
			if (properties != null)
			{
				revAttachments = (IDictionary<string, object>)properties.Get("_attachments");
			}
			if (revAttachments == null || revAttachments.Count == 0 || rev.IsDeleted())
			{
				return;
			}
			foreach (string name in revAttachments.Keys)
			{
				CBLAttachmentInternal attachment = attachments.Get(name);
				if (attachment != null)
				{
					// Determine the revpos, i.e. generation # this was added in. Usually this is
					// implicit, but a rev being pulled in replication will have it set already.
					if (attachment.GetRevpos() == 0)
					{
						attachment.SetRevpos(generation);
					}
					else
					{
						if (attachment.GetRevpos() > generation)
						{
							Log.W(Couchbase.CBLDatabase.Tag, string.Format("Attachment %s %s has unexpected revpos %s, setting to %s"
								, rev, name, attachment.GetRevpos(), generation));
							attachment.SetRevpos(generation);
						}
					}
					// Finally insert the attachment:
					InsertAttachmentForSequence(attachment, newSequence);
				}
				else
				{
					// It's just a stub, so copy the previous revision's attachment entry:
					//? Should I enforce that the type and digest (if any) match?
					CopyAttachmentNamedFromSequenceToSequence(name, parentSequence, newSequence);
				}
			}
		}

		/// <exception cref="Couchbase.CBLiteException"></exception>
		public virtual CBLRevisionInternal UpdateAttachment(string filename, InputStream 
			contentStream, string contentType, string docID, string oldRevID)
		{
			return UpdateAttachment(filename, contentStream, contentType, docID, oldRevID, null
				);
		}

		/// <summary>Updates or deletes an attachment, creating a new document revision in the process.
		/// 	</summary>
		/// <remarks>
		/// Updates or deletes an attachment, creating a new document revision in the process.
		/// Used by the PUT / DELETE methods called on attachment URLs.
		/// </remarks>
		/// <exception cref="Couchbase.CBLiteException"></exception>
		public virtual CBLRevisionInternal UpdateAttachment(string filename, InputStream 
			contentStream, string contentType, string docID, string oldRevID, CBLStatus status
			)
		{
			if (filename == null || filename.Length == 0 || (contentStream != null && contentType
				 == null) || (oldRevID != null && docID == null) || (contentStream != null && docID
				 == null))
			{
				throw new CBLiteException(CBLStatus.BadRequest);
			}
			BeginTransaction();
			try
			{
				CBLRevisionInternal oldRev = new CBLRevisionInternal(docID, oldRevID, false, this
					);
				if (oldRevID != null)
				{
					// Load existing revision if this is a replacement:
					try
					{
						LoadRevisionBody(oldRev, EnumSet.NoneOf<CBLDatabase.TDContentOptions>());
					}
					catch (CBLiteException e)
					{
						if (e.GetCBLStatus().GetCode() == CBLStatus.NotFound && ExistsDocumentWithIDAndRev
							(docID, null))
						{
							throw new CBLiteException(CBLStatus.Conflict);
						}
					}
					IDictionary<string, object> attachments = (IDictionary<string, object>)oldRev.GetProperties
						().Get("_attachments");
					if (contentStream == null && attachments != null && !attachments.ContainsKey(filename
						))
					{
						throw new CBLiteException(CBLStatus.NotFound);
					}
					// Remove the _attachments stubs so putRevision: doesn't copy the rows for me
					// OPT: Would be better if I could tell loadRevisionBody: not to add it
					if (attachments != null)
					{
						IDictionary<string, object> properties = new Dictionary<string, object>(oldRev.GetProperties
							());
						Sharpen.Collections.Remove(properties, "_attachments");
						oldRev.SetBody(new CBLBody(properties));
					}
				}
				else
				{
					// If this creates a new doc, it needs a body:
					oldRev.SetBody(new CBLBody(new Dictionary<string, object>()));
				}
				// Create a new revision:
				CBLStatus putStatus = new CBLStatus();
				CBLRevisionInternal newRev = PutRevision(oldRev, oldRevID, false, putStatus);
				if (newRev == null)
				{
					return null;
				}
				if (oldRevID != null)
				{
					// Copy all attachment rows _except_ for the one being updated:
					string[] args = new string[] { System.Convert.ToString(newRev.GetSequence()), System.Convert.ToString
						(oldRev.GetSequence()), filename };
					sqliteDb.ExecSQL("INSERT INTO attachments " + "(sequence, filename, key, type, length, revpos) "
						 + "SELECT ?, filename, key, type, length, revpos FROM attachments " + "WHERE sequence=? AND filename != ?"
						, args);
				}
				if (contentStream != null)
				{
					// If not deleting, add a new attachment entry:
					InsertAttachmentForSequenceWithNameAndType(contentStream, newRev.GetSequence(), filename
						, contentType, newRev.GetGeneration());
				}
				status.SetCode((contentStream != null) ? CBLStatus.Created : CBLStatus.Ok);
				return newRev;
			}
			catch (SQLException e)
			{
				Log.E(Tag, "Error uploading attachment", e);
				status.SetCode(CBLStatus.InternalServerError);
				return null;
			}
			finally
			{
				EndTransaction(status.IsSuccessful());
			}
		}

		public virtual void RememberAttachmentWritersForDigests(IDictionary<string, CBLBlobStoreWriter
			> blobsByDigest)
		{
			GetPendingAttachmentsByDigest().PutAll(blobsByDigest);
		}

		internal virtual void RememberAttachmentWriter(CBLBlobStoreWriter writer)
		{
			GetPendingAttachmentsByDigest().Put(writer.MD5DigestString(), writer);
		}

		/// <summary>Deletes obsolete attachments from the sqliteDb and blob store.</summary>
		/// <remarks>Deletes obsolete attachments from the sqliteDb and blob store.</remarks>
		public virtual CBLStatus GarbageCollectAttachments()
		{
			// First delete attachment rows for already-cleared revisions:
			// OPT: Could start after last sequence# we GC'd up to
			try
			{
				sqliteDb.ExecSQL("DELETE FROM attachments WHERE sequence IN " + "(SELECT sequence from revs WHERE json IS null)"
					);
			}
			catch (SQLException e)
			{
				Log.E(Couchbase.CBLDatabase.Tag, "Error deleting attachments", e);
			}
			// Now collect all remaining attachment IDs and tell the store to delete all but these:
			Cursor cursor = null;
			try
			{
				cursor = sqliteDb.RawQuery("SELECT DISTINCT key FROM attachments", null);
				cursor.MoveToFirst();
				IList<CBLBlobKey> allKeys = new AList<CBLBlobKey>();
				while (!cursor.IsAfterLast())
				{
					CBLBlobKey key = new CBLBlobKey(cursor.GetBlob(0));
					allKeys.AddItem(key);
					cursor.MoveToNext();
				}
				int numDeleted = attachments.DeleteBlobsExceptWithKeys(allKeys);
				if (numDeleted < 0)
				{
					return new CBLStatus(CBLStatus.InternalServerError);
				}
				Log.V(Couchbase.CBLDatabase.Tag, "Deleted " + numDeleted + " attachments");
				return new CBLStatus(CBLStatus.Ok);
			}
			catch (SQLException e)
			{
				Log.E(Couchbase.CBLDatabase.Tag, "Error finding attachment keys in use", e);
				return new CBLStatus(CBLStatus.InternalServerError);
			}
			finally
			{
				if (cursor != null)
				{
					cursor.Close();
				}
			}
		}

		/// <summary>DOCUMENT & REV IDS:</summary>
		public static bool IsValidDocumentId(string id)
		{
			// http://wiki.apache.org/couchdb/HTTP_Document_API#Documents
			if (id == null || id.Length == 0)
			{
				return false;
			}
			if (id[0] == '_')
			{
				return (id.StartsWith("_design/"));
			}
			return true;
		}

		// "_local/*" is not a valid document ID. Local docs have their own API and shouldn't get here.
		public static string GenerateDocumentId()
		{
			return CBLMisc.TDCreateUUID();
		}

		public virtual string GenerateNextRevisionID(string revisionId)
		{
			// Revision IDs have a generation count, a hyphen, and a UUID.
			int generation = 0;
			if (revisionId != null)
			{
				generation = CBLRevisionInternal.GenerationFromRevID(revisionId);
				if (generation == 0)
				{
					return null;
				}
			}
			string digest = CBLMisc.TDCreateUUID();
			// TODO: Generate canonical digest of body
			return Sharpen.Extensions.ToString(generation + 1) + "-" + digest;
		}

		public virtual long InsertDocumentID(string docId)
		{
			long rowId = -1;
			try
			{
				ContentValues args = new ContentValues();
				args.Put("docid", docId);
				rowId = sqliteDb.Insert("docs", null, args);
			}
			catch (Exception e)
			{
				Log.E(Couchbase.CBLDatabase.Tag, "Error inserting document id", e);
			}
			return rowId;
		}

		public virtual long GetOrInsertDocNumericID(string docId)
		{
			long docNumericId = GetDocNumericID(docId);
			if (docNumericId == 0)
			{
				docNumericId = InsertDocumentID(docId);
			}
			return docNumericId;
		}

		/// <summary>Parses the _revisions dict from a document into an array of revision ID strings
		/// 	</summary>
		public static IList<string> ParseCouchDBRevisionHistory(IDictionary<string, object
			> docProperties)
		{
			IDictionary<string, object> revisions = (IDictionary<string, object>)docProperties
				.Get("_revisions");
			if (revisions == null)
			{
				return null;
			}
			IList<string> revIDs = (IList<string>)revisions.Get("ids");
			int start = (int)revisions.Get("start");
			if (start != null)
			{
				for (int i = 0; i < revIDs.Count; i++)
				{
					string revID = revIDs[i];
					revIDs.Set(i, Sharpen.Extensions.ToString(start--) + "-" + revID);
				}
			}
			return revIDs;
		}

		/// <summary>INSERTION:</summary>
		public virtual byte[] EncodeDocumentJSON(CBLRevisionInternal rev)
		{
			IDictionary<string, object> origProps = rev.GetProperties();
			if (origProps == null)
			{
				return null;
			}
			// Don't allow any "_"-prefixed keys. Known ones we'll ignore, unknown ones are an error.
			IDictionary<string, object> properties = new Dictionary<string, object>(origProps
				.Count);
			foreach (string key in origProps.Keys)
			{
				if (key.StartsWith("_"))
				{
					if (!KnownSpecialKeys.Contains(key))
					{
						Log.E(Tag, "CBLDatabase: Invalid top-level key '" + key + "' in document to be inserted"
							);
						return null;
					}
				}
				else
				{
					properties.Put(key, origProps.Get(key));
				}
			}
			byte[] json = null;
			try
			{
				json = CBLServer.GetObjectMapper().WriteValueAsBytes(properties);
			}
			catch (Exception e)
			{
				Log.E(Couchbase.CBLDatabase.Tag, "Error serializing " + rev + " to JSON", e);
			}
			return json;
		}

		public virtual void NotifyChange(CBLRevisionInternal rev, Uri source)
		{
			IDictionary<string, object> changeNotification = new Dictionary<string, object>();
			changeNotification.Put("rev", rev);
			changeNotification.Put("seq", rev.GetSequence());
			if (source != null)
			{
				changeNotification.Put("source", source);
			}
			SetChanged();
			NotifyObservers(changeNotification);
		}

		public virtual long InsertRevision(CBLRevisionInternal rev, long docNumericID, long
			 parentSequence, bool current, byte[] data)
		{
			long rowId = 0;
			try
			{
				ContentValues args = new ContentValues();
				args.Put("doc_id", docNumericID);
				args.Put("revid", rev.GetRevId());
				if (parentSequence != 0)
				{
					args.Put("parent", parentSequence);
				}
				args.Put("current", current);
				args.Put("deleted", rev.IsDeleted());
				args.Put("json", data);
				rowId = sqliteDb.Insert("revs", null, args);
				rev.SetSequence(rowId);
			}
			catch (Exception e)
			{
				Log.E(Couchbase.CBLDatabase.Tag, "Error inserting revision", e);
			}
			return rowId;
		}

		// TODO: move this to internal API
		/// <exception cref="Couchbase.CBLiteException"></exception>
		public virtual CBLRevisionInternal PutRevision(CBLRevisionInternal rev, string prevRevId
			, CBLStatus resultStatus)
		{
			return PutRevision(rev, prevRevId, false, resultStatus);
		}

		/// <exception cref="Couchbase.CBLiteException"></exception>
		public virtual CBLRevisionInternal PutRevision(CBLRevisionInternal rev, string prevRevId
			, bool allowConflict)
		{
			CBLStatus ignoredStatus = new CBLStatus();
			return PutRevision(rev, prevRevId, allowConflict, ignoredStatus);
		}

		/// <summary>Stores a new (or initial) revision of a document.</summary>
		/// <remarks>
		/// Stores a new (or initial) revision of a document.
		/// This is what's invoked by a PUT or POST. As with those, the previous revision ID must be supplied when necessary and the call will fail if it doesn't match.
		/// </remarks>
		/// <param name="rev">The revision to add. If the docID is null, a new UUID will be assigned. Its revID must be null. It must have a JSON body.
		/// 	</param>
		/// <param name="prevRevId">The ID of the revision to replace (same as the "?rev=" parameter to a PUT), or null if this is a new document.
		/// 	</param>
		/// <param name="allowConflict">If false, an error status 409 will be returned if the insertion would create a conflict, i.e. if the previous revision already has a child.
		/// 	</param>
		/// <param name="resultStatus">On return, an HTTP status code indicating success or failure.
		/// 	</param>
		/// <returns>A new CBLRevisionInternal with the docID, revID and sequence filled in (but no body).
		/// 	</returns>
		/// <exception cref="Couchbase.CBLiteException"></exception>
		public virtual CBLRevisionInternal PutRevision(CBLRevisionInternal rev, string prevRevId
			, bool allowConflict, CBLStatus resultStatus)
		{
			// prevRevId is the rev ID being replaced, or nil if an insert
			string docId = rev.GetDocId();
			bool deleted = rev.IsDeleted();
			if ((rev == null) || ((prevRevId != null) && (docId == null)) || (deleted && (docId
				 == null)) || ((docId != null) && !IsValidDocumentId(docId)))
			{
				throw new CBLiteException(CBLStatus.BadRequest);
			}
			BeginTransaction();
			Cursor cursor = null;
			//// PART I: In which are performed lookups and validations prior to the insert...
			long docNumericID = (docId != null) ? GetDocNumericID(docId) : 0;
			long parentSequence = 0;
			try
			{
				if (prevRevId != null)
				{
					// Replacing: make sure given prevRevID is current & find its sequence number:
					if (docNumericID <= 0)
					{
						throw new CBLiteException(CBLStatus.NotFound);
					}
					string[] args = new string[] { System.Convert.ToString(docNumericID), prevRevId };
					string additionalWhereClause = string.Empty;
					if (!allowConflict)
					{
						additionalWhereClause = "AND current=1";
					}
					cursor = sqliteDb.RawQuery("SELECT sequence FROM revs WHERE doc_id=? AND revid=? "
						 + additionalWhereClause + " LIMIT 1", args);
					if (cursor.MoveToFirst())
					{
						parentSequence = cursor.GetLong(0);
					}
					if (parentSequence == 0)
					{
						// Not found: either a 404 or a 409, depending on whether there is any current revision
						if (!allowConflict && ExistsDocumentWithIDAndRev(docId, null))
						{
							throw new CBLiteException(CBLStatus.Conflict);
						}
						else
						{
							throw new CBLiteException(CBLStatus.NotFound);
						}
					}
					if (validations != null && validations.Count > 0)
					{
						// Fetch the previous revision and validate the new one against it:
						CBLRevisionInternal prevRev = new CBLRevisionInternal(docId, prevRevId, false, this
							);
						ValidateRevision(rev, prevRev);
					}
					// Make replaced rev non-current:
					ContentValues updateContent = new ContentValues();
					updateContent.Put("current", 0);
					sqliteDb.Update("revs", updateContent, "sequence=" + parentSequence, null);
				}
				else
				{
					// Inserting first revision.
					if (deleted && (docId != null))
					{
						// Didn't specify a revision to delete: 404 or a 409, depending
						if (ExistsDocumentWithIDAndRev(docId, null))
						{
							throw new CBLiteException(CBLStatus.Conflict);
						}
						else
						{
							throw new CBLiteException(CBLStatus.NotFound);
						}
					}
					// Validate:
					ValidateRevision(rev, null);
					if (docId != null)
					{
						// Inserting first revision, with docID given (PUT):
						if (docNumericID <= 0)
						{
							// Doc doesn't exist at all; create it:
							docNumericID = InsertDocumentID(docId);
							if (docNumericID <= 0)
							{
								return null;
							}
						}
						else
						{
							// Doc exists; check whether current winning revision is deleted:
							string[] args = new string[] { System.Convert.ToString(docNumericID) };
							cursor = sqliteDb.RawQuery("SELECT sequence, deleted FROM revs WHERE doc_id=? and current=1 ORDER BY revid DESC LIMIT 1"
								, args);
							if (cursor.MoveToFirst())
							{
								bool wasAlreadyDeleted = (cursor.GetInt(1) > 0);
								if (wasAlreadyDeleted)
								{
									// Make the deleted revision no longer current:
									ContentValues updateContent = new ContentValues();
									updateContent.Put("current", 0);
									sqliteDb.Update("revs", updateContent, "sequence=" + cursor.GetLong(0), null);
								}
								else
								{
									if (!allowConflict)
									{
										// docId already exists, current not deleted, conflict
										throw new CBLiteException(CBLStatus.Conflict);
									}
								}
							}
						}
					}
					else
					{
						// Inserting first revision, with no docID given (POST): generate a unique docID:
						docId = Couchbase.CBLDatabase.GenerateDocumentId();
						docNumericID = InsertDocumentID(docId);
						if (docNumericID <= 0)
						{
							return null;
						}
					}
				}
				//// PART II: In which insertion occurs...
				// Get the attachments:
				IDictionary<string, CBLAttachmentInternal> attachments = GetAttachmentsFromRevision
					(rev);
				// Bump the revID and update the JSON:
				string newRevId = GenerateNextRevisionID(prevRevId);
				byte[] data = null;
				if (!rev.IsDeleted())
				{
					data = EncodeDocumentJSON(rev);
					if (data == null)
					{
						// bad or missing json
						throw new CBLiteException(CBLStatus.BadRequest);
					}
				}
				rev = rev.CopyWithDocID(docId, newRevId);
				StubOutAttachmentsInRevision(attachments, rev);
				// Now insert the rev itself:
				long newSequence = InsertRevision(rev, docNumericID, parentSequence, true, data);
				if (newSequence == 0)
				{
					return null;
				}
				// Store any attachments:
				if (attachments != null)
				{
					ProcessAttachmentsForRevision(attachments, rev, parentSequence);
				}
				// Success!
				if (deleted)
				{
					resultStatus.SetCode(CBLStatus.Ok);
				}
				else
				{
					resultStatus.SetCode(CBLStatus.Created);
				}
			}
			catch (SQLException e1)
			{
				Log.E(Couchbase.CBLDatabase.Tag, "Error putting revision", e1);
				return null;
			}
			finally
			{
				if (cursor != null)
				{
					cursor.Close();
				}
				EndTransaction(resultStatus.IsSuccessful());
			}
			//// EPILOGUE: A change notification is sent...
			NotifyChange(rev, null);
			return rev;
		}

		/// <summary>
		/// Given a revision, read its _attachments dictionary (if any), convert each attachment to a
		/// CBLAttachmentInternal object, and return a dictionary mapping names-&gt;CBL_Attachments.
		/// </summary>
		/// <remarks>
		/// Given a revision, read its _attachments dictionary (if any), convert each attachment to a
		/// CBLAttachmentInternal object, and return a dictionary mapping names-&gt;CBL_Attachments.
		/// </remarks>
		/// <exception cref="Couchbase.CBLiteException"></exception>
		internal virtual IDictionary<string, CBLAttachmentInternal> GetAttachmentsFromRevision
			(CBLRevisionInternal rev)
		{
			IDictionary<string, object> revAttachments = (IDictionary<string, object>)rev.GetPropertyForKey
				("_attachmments");
			if (revAttachments == null || revAttachments.Count == 0 || rev.IsDeleted())
			{
				return new Dictionary<string, CBLAttachmentInternal>();
			}
			IDictionary<string, CBLAttachmentInternal> attachments = new Dictionary<string, CBLAttachmentInternal
				>();
			foreach (string name in revAttachments.Keys)
			{
				IDictionary<string, object> attachInfo = (IDictionary<string, object>)revAttachments
					.Get(name);
				string contentType = (string)attachInfo.Get("content_type");
				CBLAttachmentInternal attachment = new CBLAttachmentInternal(name, contentType);
				string newContentBase64 = (string)attachInfo.Get("data");
				if (newContentBase64 != null)
				{
					// If there's inline attachment data, decode and store it:
					byte[] newContents;
					try
					{
						newContents = Base64.Decode(newContentBase64);
					}
					catch (IOException e)
					{
						throw new CBLiteException(e, CBLStatus.BadEncoding);
					}
					attachment.SetLength(newContents.Length);
					bool storedBlob = GetAttachments().StoreBlob(newContents, attachment.GetBlobKey()
						);
					if (!storedBlob)
					{
						throw new CBLiteException(CBLStatus.StatusAttachmentError);
					}
				}
				else
				{
					if (((bool)attachInfo.Get("follows")) == true)
					{
						// "follows" means the uploader provided the attachment in a separate MIME part.
						// This means it's already been registered in _pendingAttachmentsByDigest;
						// I just need to look it up by its "digest" property and install it into the store:
						InstallAttachment(attachment, attachInfo);
					}
					else
					{
						// This item is just a stub; validate and skip it
						if (((bool)attachInfo.Get("stub")) == false)
						{
							throw new CBLiteException("Expected this attachment to be a stub", CBLStatus.BadAttachment
								);
						}
						int revPos = ((int)attachInfo.Get("revpos"));
						if (revPos <= 0)
						{
							throw new CBLiteException("Invalid revpos: " + revPos, CBLStatus.BadAttachment);
						}
						continue;
					}
				}
				// Handle encoded attachment:
				string encodingStr = (string)attachInfo.Get("encoding");
				if (encodingStr != null && encodingStr.Length > 0)
				{
					if (Sharpen.Runtime.EqualsIgnoreCase(encodingStr, "gzip"))
					{
						attachment.SetEncoding(CBLAttachmentInternal.CBLAttachmentEncoding.CBLAttachmentEncodingGZIP
							);
					}
					else
					{
						throw new CBLiteException("Unnkown encoding: " + encodingStr, CBLStatus.BadEncoding
							);
					}
					attachment.SetEncodedLength(attachment.GetLength());
					attachment.SetLength((long)attachInfo.Get("length"));
				}
				attachment.SetRevpos((int)attachInfo.Get("revpos"));
				attachments.Put(name, attachment);
			}
			return attachments;
		}

		/// <summary>Inserts an already-existing revision replicated from a remote sqliteDb.</summary>
		/// <remarks>
		/// Inserts an already-existing revision replicated from a remote sqliteDb.
		/// It must already have a revision ID. This may create a conflict! The revision's history must be given; ancestor revision IDs that don't already exist locally will create phantom revisions with no content.
		/// </remarks>
		/// <exception cref="Couchbase.CBLiteException"></exception>
		public virtual void ForceInsert(CBLRevisionInternal rev, IList<string> revHistory
			, Uri source)
		{
			string docId = rev.GetDocId();
			string revId = rev.GetRevId();
			if (!IsValidDocumentId(docId) || (revId == null))
			{
				throw new CBLiteException(CBLStatus.BadRequest);
			}
			int historyCount = 0;
			if (revHistory != null)
			{
				historyCount = revHistory.Count;
			}
			if (historyCount == 0)
			{
				revHistory = new AList<string>();
				revHistory.AddItem(revId);
				historyCount = 1;
			}
			else
			{
				if (!revHistory[0].Equals(rev.GetRevId()))
				{
					throw new CBLiteException(CBLStatus.BadRequest);
				}
			}
			bool success = false;
			BeginTransaction();
			try
			{
				// First look up all locally-known revisions of this document:
				long docNumericID = GetOrInsertDocNumericID(docId);
				CBLRevisionList localRevs = GetAllRevisionsOfDocumentID(docId, docNumericID, false
					);
				if (localRevs == null)
				{
					throw new CBLiteException(CBLStatus.InternalServerError);
				}
				// Walk through the remote history in chronological order, matching each revision ID to
				// a local revision. When the list diverges, start creating blank local revisions to fill
				// in the local history:
				long sequence = 0;
				long localParentSequence = 0;
				for (int i = revHistory.Count - 1; i >= 0; --i)
				{
					revId = revHistory[i];
					CBLRevisionInternal localRev = localRevs.RevWithDocIdAndRevId(docId, revId);
					if (localRev != null)
					{
						// This revision is known locally. Remember its sequence as the parent of the next one:
						sequence = localRev.GetSequence();
						System.Diagnostics.Debug.Assert((sequence > 0));
						localParentSequence = sequence;
					}
					else
					{
						// This revision isn't known, so add it:
						CBLRevisionInternal newRev;
						byte[] data = null;
						bool current = false;
						if (i == 0)
						{
							// Hey, this is the leaf revision we're inserting:
							newRev = rev;
							if (!rev.IsDeleted())
							{
								data = EncodeDocumentJSON(rev);
								if (data == null)
								{
									throw new CBLiteException(CBLStatus.BadRequest);
								}
							}
							current = true;
						}
						else
						{
							// It's an intermediate parent, so insert a stub:
							newRev = new CBLRevisionInternal(docId, revId, false, this);
						}
						// Insert it:
						sequence = InsertRevision(newRev, docNumericID, sequence, current, data);
						if (sequence <= 0)
						{
							throw new CBLiteException(CBLStatus.InternalServerError);
						}
						if (i == 0)
						{
							// Write any changed attachments for the new revision. As the parent sequence use
							// the latest local revision (this is to copy attachments from):
							IDictionary<string, CBLAttachmentInternal> attachments = GetAttachmentsFromRevision
								(rev);
							if (attachments != null)
							{
								ProcessAttachmentsForRevision(attachments, rev, localParentSequence);
								StubOutAttachmentsInRevision(attachments, rev);
							}
						}
					}
				}
				// Mark the latest local rev as no longer current:
				if (localParentSequence > 0 && localParentSequence != sequence)
				{
					ContentValues args = new ContentValues();
					args.Put("current", 0);
					string[] whereArgs = new string[] { System.Convert.ToString(localParentSequence) };
					try
					{
						sqliteDb.Update("revs", args, "sequence=?", whereArgs);
					}
					catch (SQLException)
					{
						throw new CBLiteException(CBLStatus.InternalServerError);
					}
				}
				success = true;
			}
			catch (SQLException)
			{
				EndTransaction(success);
				throw new CBLiteException(CBLStatus.InternalServerError);
			}
			finally
			{
				EndTransaction(success);
			}
			// Notify and return:
			NotifyChange(rev, source);
		}

		/// <summary>Defines or clears a named document validation function.</summary>
		/// <remarks>
		/// Defines or clears a named document validation function.
		/// Before any change to the database, all registered validation functions are called and given a
		/// chance to reject it. (This includes incoming changes from a pull replication.)
		/// </remarks>
		public virtual void DefineValidation(string name, CBLValidationBlock validationBlock
			)
		{
			if (validations == null)
			{
				validations = new Dictionary<string, CBLValidationBlock>();
			}
			if (validationBlock != null)
			{
				validations.Put(name, validationBlock);
			}
			else
			{
				Sharpen.Collections.Remove(validations, name);
			}
		}

		/// <summary>Returns the existing document validation function (block) registered with the given name.
		/// 	</summary>
		/// <remarks>
		/// Returns the existing document validation function (block) registered with the given name.
		/// Note that validations are not persistent -- you have to re-register them on every launch.
		/// </remarks>
		public virtual CBLValidationBlock GetValidation(string name)
		{
			CBLValidationBlock result = null;
			if (validations != null)
			{
				result = validations.Get(name);
			}
			return result;
		}

		/// <exception cref="Couchbase.CBLiteException"></exception>
		public virtual void ValidateRevision(CBLRevisionInternal newRev, CBLRevisionInternal
			 oldRev)
		{
			if (validations == null || validations.Count == 0)
			{
				return;
			}
			TDValidationContextImpl context = new TDValidationContextImpl(this, oldRev);
			foreach (string validationName in validations.Keys)
			{
				CBLValidationBlock validation = GetValidation(validationName);
				if (!validation.Validate(newRev, context))
				{
					throw new CBLiteException(context.GetErrorType().GetCode());
				}
			}
		}

		//TODO implement missing replication methods
		/// <summary>Get all the replicators associated with this database.</summary>
		/// <remarks>Get all the replicators associated with this database.</remarks>
		/// <returns></returns>
		public virtual IList<CBLReplicator> GetAllReplications()
		{
			return activeReplicators;
		}

		/// <summary>
		/// Creates a replication that will 'push' to a database at the given URL, or returns an existing
		/// such replication if there already is one.
		/// </summary>
		/// <remarks>
		/// Creates a replication that will 'push' to a database at the given URL, or returns an existing
		/// such replication if there already is one.
		/// </remarks>
		/// <param name="remote"></param>
		/// <returns></returns>
		public virtual CBLReplicator Push(Uri remote)
		{
			return GetActiveReplicator(remote, true);
		}

		/// <summary>
		/// Creates a replication that will 'pull' from a database at the given URL, or returns an existing
		/// such replication if there already is one.
		/// </summary>
		/// <remarks>
		/// Creates a replication that will 'pull' from a database at the given URL, or returns an existing
		/// such replication if there already is one.
		/// </remarks>
		/// <param name="remote"></param>
		/// <returns></returns>
		public virtual CBLReplicator Pull(Uri remote)
		{
			return GetActiveReplicator(remote, false);
		}

		/// <summary>
		/// Creates a pair of replications to both pull and push to database at the given URL, or
		/// returns existing replications if there are any.
		/// </summary>
		/// <remarks>
		/// Creates a pair of replications to both pull and push to database at the given URL, or
		/// returns existing replications if there are any.
		/// </remarks>
		/// <param name="remote"></param>
		/// <param name="exclusively">- this param is ignored!  TODO: fix this</param>
		/// <returns>An array whose first element is the "pull" replication and second is the "push".
		/// 	</returns>
		public virtual IList<CBLReplicator> Replicate(Uri remote, bool exclusively)
		{
			CBLReplicator pull;
			CBLReplicator push;
			if (remote != null)
			{
				pull = Pull(remote);
				push = Push(remote);
				AList<CBLReplicator> result = new AList<CBLReplicator>();
				result.AddItem(pull);
				result.AddItem(push);
				return result;
			}
			return null;
		}

		public virtual CBLReplicator GetActiveReplicator(Uri remote, bool push)
		{
			if (activeReplicators != null)
			{
				foreach (CBLReplicator replicator in activeReplicators)
				{
					if (replicator.GetRemote().Equals(remote) && replicator.IsPush() == push && replicator
						.IsRunning())
					{
						return replicator;
					}
				}
			}
			return null;
		}

		public virtual CBLReplicator GetReplicator(Uri remote, bool push, bool continuous
			, ScheduledExecutorService workExecutor)
		{
			CBLReplicator replicator = GetReplicator(remote, null, push, continuous, workExecutor
				);
			return replicator;
		}

		public virtual CBLReplicator GetReplicator(string sessionId)
		{
			if (activeReplicators != null)
			{
				foreach (CBLReplicator replicator in activeReplicators)
				{
					if (replicator.GetSessionID().Equals(sessionId))
					{
						return replicator;
					}
				}
			}
			return null;
		}

		public virtual CBLReplicator GetReplicator(Uri remote, HttpClientFactory httpClientFactory
			, bool push, bool continuous, ScheduledExecutorService workExecutor)
		{
			CBLReplicator result = GetActiveReplicator(remote, push);
			if (result != null)
			{
				return result;
			}
			result = push ? new CBLPusher(this, remote, continuous, httpClientFactory, workExecutor
				) : new CBLPuller(this, remote, continuous, httpClientFactory, workExecutor);
			if (activeReplicators == null)
			{
				activeReplicators = new AList<CBLReplicator>();
			}
			activeReplicators.AddItem(result);
			return result;
		}

		public virtual string LastSequenceWithRemoteURL(Uri url, bool push)
		{
			Cursor cursor = null;
			string result = null;
			try
			{
				string[] args = new string[] { url.ToExternalForm(), Sharpen.Extensions.ToString(
					push ? 1 : 0) };
				cursor = sqliteDb.RawQuery("SELECT last_sequence FROM replicators WHERE remote=? AND push=?"
					, args);
				if (cursor.MoveToFirst())
				{
					result = cursor.GetString(0);
				}
			}
			catch (SQLException e)
			{
				Log.E(Couchbase.CBLDatabase.Tag, "Error getting last sequence", e);
				return null;
			}
			finally
			{
				if (cursor != null)
				{
					cursor.Close();
				}
			}
			return result;
		}

		public virtual bool SetLastSequence(string lastSequence, Uri url, bool push)
		{
			ContentValues values = new ContentValues();
			values.Put("remote", url.ToExternalForm());
			values.Put("push", push);
			values.Put("last_sequence", lastSequence);
			long newId = sqliteDb.InsertWithOnConflict("replicators", null, values, SQLiteDatabase
				.ConflictReplace);
			return (newId == -1);
		}

		public static string Quote(string @string)
		{
			return @string.Replace("'", "''");
		}

		public static string JoinQuotedObjects(IList<object> objects)
		{
			IList<string> strings = new AList<string>();
			foreach (object @object in objects)
			{
				strings.AddItem(@object != null ? @object.ToString() : null);
			}
			return JoinQuoted(strings);
		}

		public static string JoinQuoted(IList<string> strings)
		{
			if (strings.Count == 0)
			{
				return string.Empty;
			}
			string result = "'";
			bool first = true;
			foreach (string @string in strings)
			{
				if (first)
				{
					first = false;
				}
				else
				{
					result = result + "','";
				}
				result = result + Quote(@string);
			}
			result = result + "'";
			return result;
		}

		public virtual bool FindMissingRevisions(CBLRevisionList touchRevs)
		{
			if (touchRevs.Count == 0)
			{
				return true;
			}
			string quotedDocIds = JoinQuoted(touchRevs.GetAllDocIds());
			string quotedRevIds = JoinQuoted(touchRevs.GetAllRevIds());
			string sql = "SELECT docid, revid FROM revs, docs " + "WHERE docid IN (" + quotedDocIds
				 + ") AND revid in (" + quotedRevIds + ")" + " AND revs.doc_id == docs.doc_id";
			Cursor cursor = null;
			try
			{
				cursor = sqliteDb.RawQuery(sql, null);
				cursor.MoveToFirst();
				while (!cursor.IsAfterLast())
				{
					CBLRevisionInternal rev = touchRevs.RevWithDocIdAndRevId(cursor.GetString(0), cursor
						.GetString(1));
					if (rev != null)
					{
						touchRevs.Remove(rev);
					}
					cursor.MoveToNext();
				}
			}
			catch (SQLException e)
			{
				Log.E(Couchbase.CBLDatabase.Tag, "Error finding missing revisions", e);
				return false;
			}
			finally
			{
				if (cursor != null)
				{
					cursor.Close();
				}
			}
			return true;
		}

		/// <summary>Returns the contents of the local document with the given ID, or nil if none exists.
		/// 	</summary>
		/// <remarks>Returns the contents of the local document with the given ID, or nil if none exists.
		/// 	</remarks>
		public virtual IDictionary<string, object> GetLocalDocument(string documentId)
		{
			return dbInternal.GetLocalDocument(MakeLocalDocumentId(documentId), null).GetProperties
				();
		}

		internal static string MakeLocalDocumentId(string documentId)
		{
			return string.Format("_local/%s", documentId);
		}

		/// <summary>Sets the contents of the local document with the given ID.</summary>
		/// <remarks>
		/// Sets the contents of the local document with the given ID. Unlike CouchDB, no revision-ID
		/// checking is done; the put always succeeds. If the properties dictionary is nil, the document
		/// will be deleted.
		/// </remarks>
		/// <exception cref="Couchbase.CBLiteException"></exception>
		public virtual bool PutLocalDocument(IDictionary<string, object> properties, string
			 id)
		{
			// TODO: there was some code in the iOS implementation equivalent that I did not know if needed
			CBLRevisionInternal prevRev = dbInternal.GetLocalDocument(id, null);
			if (prevRev == null && properties == null)
			{
				return false;
			}
			return PutLocalRevision(prevRev, prevRev.GetRevId()) != null;
		}

		/// <summary>
		/// Don't use this method!  This should be considered a private API for the Couchbase Lite
		/// library to use internally.
		/// </summary>
		/// <remarks>
		/// Don't use this method!  This should be considered a private API for the Couchbase Lite
		/// library to use internally.
		/// </remarks>
		/// <returns></returns>
		public virtual CBLDatabaseInternal GetDbInternal()
		{
			return dbInternal;
		}

		/// <exception cref="Couchbase.CBLiteException"></exception>
		public virtual CBLRevisionInternal PutLocalRevision(CBLRevisionInternal revision, 
			string prevRevID)
		{
			string docID = revision.GetDocId();
			if (!docID.StartsWith("_local/"))
			{
				throw new CBLiteException(CBLStatus.BadRequest);
			}
			if (!revision.IsDeleted())
			{
				// PUT:
				byte[] json = EncodeDocumentJSON(revision);
				string newRevID;
				if (prevRevID != null)
				{
					int generation = CBLRevisionInternal.GenerationFromRevID(prevRevID);
					if (generation == 0)
					{
						throw new CBLiteException(CBLStatus.BadRequest);
					}
					newRevID = Sharpen.Extensions.ToString(++generation) + "-local";
					ContentValues values = new ContentValues();
					values.Put("revid", newRevID);
					values.Put("json", json);
					string[] whereArgs = new string[] { docID, prevRevID };
					try
					{
						int rowsUpdated = sqliteDb.Update("localdocs", values, "docid=? AND revid=?", whereArgs
							);
						if (rowsUpdated == 0)
						{
							throw new CBLiteException(CBLStatus.Conflict);
						}
					}
					catch (SQLException e)
					{
						throw new CBLiteException(e, CBLStatus.InternalServerError);
					}
				}
				else
				{
					newRevID = "1-local";
					ContentValues values = new ContentValues();
					values.Put("docid", docID);
					values.Put("revid", newRevID);
					values.Put("json", json);
					try
					{
						sqliteDb.InsertWithOnConflict("localdocs", null, values, SQLiteDatabase.ConflictIgnore
							);
					}
					catch (SQLException e)
					{
						throw new CBLiteException(e, CBLStatus.InternalServerError);
					}
				}
				return revision.CopyWithDocID(docID, newRevID);
			}
			else
			{
				// DELETE:
				dbInternal.DeleteLocalDocument(docID, prevRevID);
				return revision;
			}
		}

		/// <summary>Deletes the local document with the given ID.</summary>
		/// <remarks>Deletes the local document with the given ID.</remarks>
		/// <exception cref="Couchbase.CBLiteException"></exception>
		public virtual bool DeleteLocalDocument(string id)
		{
			CBLRevisionInternal prevRev = dbInternal.GetLocalDocument(id, null);
			if (prevRev == null)
			{
				return false;
			}
			dbInternal.DeleteLocalDocument(id, prevRev.GetRevId());
			return true;
		}

		/// <summary>Returns a query that matches all documents in the database.</summary>
		/// <remarks>
		/// Returns a query that matches all documents in the database.
		/// This is like querying an imaginary view that emits every document's ID as a key.
		/// </remarks>
		public virtual CBLQuery QueryAllDocuments()
		{
			return new CBLQuery(this, (CBLView)null);
		}

		/// <summary>Creates a one-shot query with the given map function.</summary>
		/// <remarks>
		/// Creates a one-shot query with the given map function. This is equivalent to creating an
		/// anonymous CBLView and then deleting it immediately after querying it. It may be useful during
		/// development, but in general this is inefficient if this map will be used more than once,
		/// because the entire view has to be regenerated from scratch every time.
		/// </remarks>
		public virtual CBLQuery SlowQuery(CBLMapFunction map)
		{
			return new CBLQuery(this, map);
		}

		/// <summary>Purges specific revisions, which deletes them completely from the local database _without_ adding a "tombstone" revision.
		/// 	</summary>
		/// <remarks>
		/// Purges specific revisions, which deletes them completely from the local database _without_ adding a "tombstone" revision. It's as though they were never there.
		/// This operation is described here: http://wiki.apache.org/couchdb/Purge_Documents
		/// </remarks>
		/// <param name="docsToRevs">A dictionary mapping document IDs to arrays of revision IDs.
		/// 	</param>
		/// <resultOn>success will point to an NSDictionary with the same form as docsToRev, containing the doc/revision IDs that were actually removed.
		/// 	</resultOn>
		internal virtual IDictionary<string, object> PurgeRevisions(IDictionary<string, IList
			<string>> docsToRevs)
		{
			IDictionary<string, object> result = new Dictionary<string, object>();
			InTransaction(new _CBLDatabaseFunction_3148(this, docsToRevs, result));
			// no such document, skip it
			// Delete all revisions if magic "*" revision ID is given:
			// Iterate over all the revisions of the doc, in reverse sequence order.
			// Keep track of all the sequences to delete, i.e. the given revs and ancestors,
			// but not any non-given leaf revs or their ancestors.
			// Purge it and maybe its parent:
			// Keep it and its parent:
			// Now delete the sequences to be purged.
			return result;
		}

		private sealed class _CBLDatabaseFunction_3148 : CBLDatabaseFunction
		{
			public _CBLDatabaseFunction_3148(CBLDatabase _enclosing, IDictionary<string, IList
				<string>> docsToRevs, IDictionary<string, object> result)
			{
				this._enclosing = _enclosing;
				this.docsToRevs = docsToRevs;
				this.result = result;
			}

			public bool PerformFunction()
			{
				foreach (string docID in docsToRevs.Keys)
				{
					long docNumericID = this._enclosing.GetDocNumericID(docID);
					if (docNumericID == -1)
					{
						continue;
					}
					IList<string> revsPurged = null;
					IList<string> revIDs = (IList<string>)docsToRevs.Get(docID);
					if (revIDs == null)
					{
						return false;
					}
					else
					{
						if (revIDs.Count == 0)
						{
							revsPurged = new AList<string>();
						}
						else
						{
							if (revIDs.Contains("*"))
							{
								try
								{
									string[] args = new string[] { System.Convert.ToString(docNumericID) };
									this._enclosing.sqliteDb.ExecSQL("DELETE FROM revs WHERE doc_id=?", args);
								}
								catch (SQLException e)
								{
									Log.E(Couchbase.CBLDatabase.Tag, "Error deleting revisions", e);
									return false;
								}
								revsPurged.AddItem("*");
							}
							else
							{
								Cursor cursor = null;
								try
								{
									string[] args = new string[] { System.Convert.ToString(docNumericID) };
									string queryString = "SELECT revid, sequence, parent FROM revs WHERE doc_id=? ORDER BY sequence DESC";
									cursor = this._enclosing.sqliteDb.RawQuery(queryString, args);
									if (!cursor.MoveToFirst())
									{
										Log.W(Couchbase.CBLDatabase.Tag, "No results for query: " + queryString);
										return false;
									}
									ICollection<long> seqsToPurge = new HashSet<long>();
									ICollection<long> seqsToKeep = new HashSet<long>();
									ICollection<string> revsToPurge = new HashSet<string>();
									while (!cursor.IsAfterLast())
									{
										string revID = cursor.GetString(0);
										long sequence = cursor.GetLong(1);
										long parent = cursor.GetLong(2);
										if (seqsToPurge.Contains(sequence) || revIDs.Contains(revID) && !seqsToKeep.Contains
											(sequence))
										{
											seqsToPurge.AddItem(sequence);
											revsToPurge.AddItem(revID);
											if (parent > 0)
											{
												seqsToPurge.AddItem(parent);
											}
										}
										else
										{
											seqsToPurge.Remove(sequence);
											revsToPurge.Remove(revID);
											seqsToKeep.AddItem(parent);
										}
										cursor.MoveToNext();
									}
									seqsToPurge.RemoveAll(seqsToKeep);
									Log.I(Couchbase.CBLDatabase.Tag, string.Format("Purging doc '%s' revs (%s); asked for (%s)"
										, docID, revsToPurge, revIDs));
									if (seqsToPurge.Count > 0)
									{
										string seqsToPurgeList = TextUtils.Join(",", seqsToPurge);
										string sql = string.Format("DELETE FROM revs WHERE sequence in (%s)", seqsToPurgeList
											);
										try
										{
											this._enclosing.sqliteDb.ExecSQL(sql);
										}
										catch (SQLException e)
										{
											Log.E(Couchbase.CBLDatabase.Tag, "Error deleting revisions via: " + sql, e);
											return false;
										}
									}
									Sharpen.Collections.AddAll(revsPurged, revsToPurge);
								}
								catch (SQLException e)
								{
									Log.E(Couchbase.CBLDatabase.Tag, "Error getting revisions", e);
									return false;
								}
								finally
								{
									if (cursor != null)
									{
										cursor.Close();
									}
								}
							}
						}
					}
					result.Put(docID, revsPurged);
				}
				return true;
			}

			private readonly CBLDatabase _enclosing;

			private readonly IDictionary<string, IList<string>> docsToRevs;

			private readonly IDictionary<string, object> result;
		}

		/// <summary>Returns the currently registered filter compiler (nil by default).</summary>
		/// <remarks>Returns the currently registered filter compiler (nil by default).</remarks>
		public virtual IDictionary<string, CBLFilterCompiler> GetFilterCompiler()
		{
			// TODO: the filter compiler currently ignored
			return filterCompiler;
		}

		/// <summary>Registers an object that can compile source code into executable filter blocks.
		/// 	</summary>
		/// <remarks>Registers an object that can compile source code into executable filter blocks.
		/// 	</remarks>
		public virtual void SetFilterCompiler(IDictionary<string, CBLFilterCompiler> filterCompiler
			)
		{
			// TODO: the filter compiler currently ignored
			this.filterCompiler = filterCompiler;
		}
	}

	internal class TDValidationContextImpl : CBLValidationContext
	{
		private CBLDatabase database;

		private CBLRevisionInternal currentRevision;

		private CBLStatus errorType;

		private string errorMessage;

		public TDValidationContextImpl(CBLDatabase database, CBLRevisionInternal currentRevision
			)
		{
			this.database = database;
			this.currentRevision = currentRevision;
			this.errorType = new CBLStatus(CBLStatus.Forbidden);
			this.errorMessage = "invalid document";
		}

		/// <exception cref="Couchbase.CBLiteException"></exception>
		public virtual CBLRevisionInternal GetCurrentRevision()
		{
			if (currentRevision != null)
			{
				database.LoadRevisionBody(currentRevision, EnumSet.NoneOf<CBLDatabase.TDContentOptions
					>());
			}
			return currentRevision;
		}

		public virtual CBLStatus GetErrorType()
		{
			return errorType;
		}

		public virtual void SetErrorType(CBLStatus status)
		{
			this.errorType = status;
		}

		public virtual string GetErrorMessage()
		{
			return errorMessage;
		}

		public virtual void SetErrorMessage(string message)
		{
			this.errorMessage = message;
		}
	}
}
