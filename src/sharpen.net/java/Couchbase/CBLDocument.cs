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

using System.Collections.Generic;
using Android.Util;
using Couchbase;
using Couchbase.Internal;
using Sharpen;

namespace Couchbase
{
	/// <summary>A CouchbaseLite document (as opposed to any specific revision of it.)</summary>
	public class CBLDocument
	{
		/// <summary>The document's owning database.</summary>
		/// <remarks>The document's owning database.</remarks>
		private CBLDatabase database;

		/// <summary>The document's ID.</summary>
		/// <remarks>The document's ID.</remarks>
		private string documentId;

		/// <summary>The current/latest revision.</summary>
		/// <remarks>The current/latest revision. This object is cached.</remarks>
		private CBLRevision currentRevision;

		/// <summary>Application-defined model object representing this document</summary>
		private object model;

		/// <summary>Constructor</summary>
		/// <param name="database">The document's owning database</param>
		/// <param name="documentId">The document's ID</param>
		public CBLDocument(CBLDatabase database, string documentId)
		{
			this.database = database;
			this.documentId = documentId;
		}

		/// <summary>Get the document's owning database.</summary>
		/// <remarks>Get the document's owning database.</remarks>
		public virtual CBLDatabase GetDatabase()
		{
			return database;
		}

		/// <summary>Get the document's ID</summary>
		public virtual string GetId()
		{
			return documentId;
		}

		/// <summary>Get the document's abbreviated ID</summary>
		public virtual string GetAbbreviatedId()
		{
			string abbreviated = documentId;
			if (documentId.Length > 10)
			{
				string firstFourChars = Sharpen.Runtime.Substring(documentId, 0, 4);
				string lastFourChars = Sharpen.Runtime.Substring(documentId, abbreviated.Length -
					 4);
				return string.Format("%s..%s", firstFourChars, lastFourChars);
			}
			return documentId;
		}

		/// <summary>Is this document deleted? (That is, does its current revision have the '_deleted' property?)
		/// 	</summary>
		/// <returns>boolean to indicate whether deleted or not</returns>
		public virtual bool IsDeleted()
		{
			return currentRevision.IsDeleted();
		}

		/// <summary>Deletes this document by adding a deletion revision.</summary>
		/// <remarks>
		/// Deletes this document by adding a deletion revision.
		/// This will be replicated to other databases.
		/// </remarks>
		/// <returns>boolean to indicate whether deleted or not</returns>
		/// <exception cref="CBLiteException">CBLiteException</exception>
		/// <exception cref="Couchbase.CBLiteException"></exception>
		public virtual bool Delete()
		{
			return currentRevision.DeleteDocument() != null;
		}

		/// <summary>Purges this document from the database; this is more than deletion, it forgets entirely about it.
		/// 	</summary>
		/// <remarks>
		/// Purges this document from the database; this is more than deletion, it forgets entirely about it.
		/// The purge will NOT be replicated to other databases.
		/// </remarks>
		/// <returns>boolean to indicate whether purged or not</returns>
		/// <exception cref="CBLiteException">CBLiteException</exception>
		/// <exception cref="Couchbase.CBLiteException"></exception>
		public virtual bool Purge()
		{
			IDictionary<string, IList<string>> docsToRevs = new Dictionary<string, IList<string
				>>();
			IList<string> revs = new AList<string>();
			revs.AddItem("*");
			docsToRevs.Put(documentId, revs);
			database.PurgeRevisions(docsToRevs);
			database.RemoveDocumentFromCache(this);
			return true;
		}

		/// <summary>The revision with the specified ID.</summary>
		/// <remarks>The revision with the specified ID.</remarks>
		/// <param name="revisionID">the revision ID</param>
		/// <returns>the CBLRevision object</returns>
		public virtual CBLRevision GetRevision(string revisionID)
		{
			if (revisionID.Equals(currentRevision.GetId()))
			{
				return currentRevision;
			}
			EnumSet<CBLDatabase.TDContentOptions> contentOptions = EnumSet.NoneOf<CBLDatabase.TDContentOptions
				>();
			CBLRevisionInternal revisionInternal = database.GetDocumentWithIDAndRev(GetId(), 
				revisionID, contentOptions);
			CBLRevision revision = null;
			revision = GetRevisionFromRev(revisionInternal);
			return revision;
		}

		/// <summary>Get the current revision</summary>
		/// <returns></returns>
		public virtual CBLRevision GetCurrentRevision()
		{
			return currentRevision;
		}

		/// <summary>Get the ID of the current revision</summary>
		/// <returns></returns>
		public virtual string GetCurrentRevisionId()
		{
			return currentRevision.GetId();
		}

		/// <summary>Returns the document's history as an array of CBLRevisions.</summary>
		/// <remarks>Returns the document's history as an array of CBLRevisions. (See CBLRevision's method.)
		/// 	</remarks>
		/// <returns>document's history</returns>
		/// <exception cref="CBLiteException">CBLiteException</exception>
		/// <exception cref="Couchbase.CBLiteException"></exception>
		public virtual IList<CBLRevision> GetRevisionHistory()
		{
			if (currentRevision == null)
			{
				Log.W(CBLDatabase.Tag, "getRevisionHistory() called but no currentRevision");
				return null;
			}
			return currentRevision.GetRevisionHistory();
		}

		/// <summary>Returns all the current conflicting revisions of the document.</summary>
		/// <remarks>
		/// Returns all the current conflicting revisions of the document. If the document is not
		/// in conflict, only the single current revision will be returned.
		/// </remarks>
		/// <returns>all current conflicting revisions of the document</returns>
		/// <exception cref="CBLiteException">CBLiteException</exception>
		/// <exception cref="Couchbase.CBLiteException"></exception>
		public virtual IList<CBLRevision> GetConflictingRevisions()
		{
			return GetLeafRevisions(false);
		}

		/// <summary>
		/// Returns all the leaf revisions in the document's revision tree,
		/// including deleted revisions (i.e.
		/// </summary>
		/// <remarks>
		/// Returns all the leaf revisions in the document's revision tree,
		/// including deleted revisions (i.e. previously-resolved conflicts.)
		/// </remarks>
		/// <returns>all the leaf revisions in the document's revision tree</returns>
		/// <exception cref="CBLiteException">CBLiteException</exception>
		/// <exception cref="Couchbase.CBLiteException"></exception>
		public virtual IList<CBLRevision> GetLeafRevisions()
		{
			return GetLeafRevisions(true);
		}

		/// <exception cref="Couchbase.CBLiteException"></exception>
		internal virtual IList<CBLRevision> GetLeafRevisions(bool includeDeleted)
		{
			IList<CBLRevision> result = new AList<CBLRevision>();
			CBLRevisionList revs = database.GetAllRevisionsOfDocumentID(documentId, true);
			foreach (CBLRevisionInternal rev in revs)
			{
				// add it to result, unless we are not supposed to include deleted and it's deleted
				if (!includeDeleted && rev.IsDeleted())
				{
				}
				else
				{
					// don't add it
					result.AddItem(GetRevisionFromRev(rev));
				}
			}
			return Sharpen.Collections.UnmodifiableList(result);
		}

		/// <summary>
		/// Creates an unsaved new revision whose parent is the currentRevision,
		/// or which will be the first revision if the document doesn't exist yet.
		/// </summary>
		/// <remarks>
		/// Creates an unsaved new revision whose parent is the currentRevision,
		/// or which will be the first revision if the document doesn't exist yet.
		/// You can modify this revision's properties and attachments, then save it.
		/// No change is made to the database until/unless you save the new revision.
		/// </remarks>
		/// <returns>the newly created revision</returns>
		public virtual CBLNewRevision NewRevision()
		{
			return new CBLNewRevision(this, currentRevision);
		}

		/// <summary>Shorthand for getProperties().get(key)</summary>
		public virtual object PropertyForKey(string key)
		{
			return currentRevision.GetProperties().Get(key);
		}

		/// <summary>The contents of the current revision of the document.</summary>
		/// <remarks>
		/// The contents of the current revision of the document.
		/// This is shorthand for self.currentRevision.properties.
		/// Any keys in the dictionary that begin with "_", such as "_id" and "_rev", contain CouchbaseLite metadata.
		/// </remarks>
		/// <returns>contents of the current revision of the document.</returns>
		public virtual IDictionary<string, object> GetProperties()
		{
			return currentRevision.GetProperties();
		}

		/// <summary>The user-defined properties, without the ones reserved by CouchDB.</summary>
		/// <remarks>
		/// The user-defined properties, without the ones reserved by CouchDB.
		/// This is based on -properties, with every key whose name starts with "_" removed.
		/// </remarks>
		/// <returns>user-defined properties, without the ones reserved by CouchDB.</returns>
		public virtual IDictionary<string, object> GetUserProperties()
		{
			return currentRevision.GetUserProperties();
		}

		/// <summary>Saves a new revision.</summary>
		/// <remarks>
		/// Saves a new revision. The properties dictionary must have a "_rev" property
		/// whose ID matches the current revision's (as it will if it's a modified
		/// copy of this document's .properties property.)
		/// </remarks>
		/// <param name="properties">the contents to be saved in the new revision</param>
		/// <returns>a new CBLRevision</returns>
		/// <exception cref="Couchbase.CBLiteException"></exception>
		public virtual CBLRevision PutProperties(IDictionary<string, object> properties)
		{
			string prevID = (string)properties.Get("_rev");
			return PutProperties(properties, prevID);
		}

		/// <exception cref="Couchbase.CBLiteException"></exception>
		internal virtual CBLRevision PutProperties(IDictionary<string, object> properties
			, string prevID)
		{
			string newId = (string)properties.Get("_id");
			if (newId != null && !Sharpen.Runtime.EqualsIgnoreCase(newId, GetId()))
			{
				Log.W(CBLDatabase.Tag, string.Format("Trying to put wrong _id to this: %s properties: %s"
					, this, properties));
			}
			// Process _attachments dict, converting CBLAttachments to dicts:
			IDictionary<string, object> attachments = (IDictionary<string, object>)properties
				.Get("_attachments");
			if (attachments != null && attachments.Count > 0)
			{
				IDictionary<string, object> updatedAttachments = CBLAttachment.InstallAttachmentBodies
					(attachments, database);
				properties.Put("_attachments", updatedAttachments);
			}
			bool deleted = (properties == null) || ((bool)properties.Get("_deleted"));
			CBLRevisionInternal rev = new CBLRevisionInternal(documentId, null, deleted, database
				);
			if (properties != null)
			{
				rev.SetProperties(properties);
			}
			CBLRevisionInternal newRev = database.PutRevision(rev, prevID, false);
			if (newRev == null)
			{
				return null;
			}
			return new CBLRevision(this, newRev);
		}

		/// <summary>Saves a new revision by letting the caller update the existing properties.
		/// 	</summary>
		/// <remarks>
		/// Saves a new revision by letting the caller update the existing properties.
		/// This method handles conflicts by retrying (calling the block again).
		/// The CBLRevisionUpdater implementation should modify the properties of the new revision and return YES to save or
		/// NO to cancel. Be careful: the CBLRevisionUpdater can be called multiple times if there is a conflict!
		/// </remarks>
		/// <param name="updater">
		/// the callback CBLRevisionUpdater implementation.  Will be called on each
		/// attempt to save. Should update the given revision's properties and then
		/// return YES, or just return NO to cancel.
		/// </param>
		/// <returns>The new saved revision, or null on error or cancellation.</returns>
		/// <exception cref="CBLiteException">CBLiteException</exception>
		/// <exception cref="Couchbase.CBLiteException"></exception>
		public virtual CBLRevision Update(CBLDocument.CBLRevisionUpdater updater)
		{
			int lastErrorCode = CBLStatus.Unknown;
			do
			{
				CBLNewRevision newRev = NewRevision();
				if (updater.UpdateRevision(newRev) == false)
				{
					break;
				}
				try
				{
					CBLRevision savedRev = newRev.Save();
					if (savedRev != null)
					{
						return savedRev;
					}
				}
				catch (CBLiteException e)
				{
					lastErrorCode = e.GetCBLStatus().GetCode();
				}
			}
			while (lastErrorCode == CBLStatus.Conflict);
			return null;
		}

		internal virtual CBLRevision GetRevisionFromRev(CBLRevisionInternal internalRevision
			)
		{
			if (internalRevision == null)
			{
				return null;
			}
			else
			{
				if (internalRevision.GetRevId().Equals(currentRevision.GetId()))
				{
					return currentRevision;
				}
				else
				{
					return new CBLRevision(this, internalRevision);
				}
			}
		}

		public virtual object GetModel()
		{
			return model;
		}

		public virtual void SetModel(object model)
		{
			this.model = model;
		}

		public interface CBLRevisionUpdater
		{
			bool UpdateRevision(CBLNewRevision newRevision);
		}

		internal virtual void LoadCurrentRevisionFrom(CBLQueryRow row)
		{
			if (row.GetDocumentRevisionId() == null)
			{
				return;
			}
			string revId = row.GetDocumentRevisionId();
			bool rowRevisionIsGreater = (CBLRevisionInternal.CBLCompareRevIDs(revId, currentRevision
				.GetId()) > 0);
			if (currentRevision == null || rowRevisionIsGreater)
			{
				IDictionary<string, object> properties = row.GetDocumentProperties();
				if (properties != null)
				{
					CBLRevisionInternal rev = new CBLRevisionInternal(properties);
					currentRevision = new CBLRevision(this, rev);
				}
			}
		}
	}
}
