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
using Couchbase;
using Sharpen;

namespace Couchbase
{
	/// <summary>Stores information about a revision -- its docID, revID, and whether it's deleted.
	/// 	</summary>
	/// <remarks>
	/// Stores information about a revision -- its docID, revID, and whether it's deleted.
	/// It can also store the sequence number and document contents (they can be added after creation).
	/// </remarks>
	public abstract class CBLRevisionBase
	{
		/// <summary>The ID of this revision.</summary>
		/// <remarks>The ID of this revision. Will be nil if this is an unsaved CBLNewRevision.
		/// 	</remarks>
		protected internal string revId;

		/// <summary>The sequence number of this revision.</summary>
		/// <remarks>The sequence number of this revision.</remarks>
		protected internal long sequence;

		/// <summary>The revisions's owning database.</summary>
		/// <remarks>The revisions's owning database.</remarks>
		protected internal CBLDatabase database;

		/// <summary>The document this is a revision of</summary>
		protected internal CBLDocument document;

		/// <summary>Constructor</summary>
		internal CBLRevisionBase() : base()
		{
		}

		/// <summary>Constructor</summary>
		/// <param name="document"></param>
		protected internal CBLRevisionBase(CBLDocument document)
		{
			this.document = document;
		}

		/// <summary>Get the revision's owning database.</summary>
		/// <remarks>Get the revision's owning database.</remarks>
		public virtual CBLDatabase GetDatabase()
		{
			return database;
		}

		/// <summary>Get the document this is a revision of</summary>
		public virtual CBLDocument GetDocument()
		{
			return document;
		}

		/// <summary>The contents of this revision of the document.</summary>
		/// <remarks>
		/// The contents of this revision of the document.
		/// Any keys in the dictionary that begin with "_", such as "_id" and "_rev", contain CouchbaseLite metadata.
		/// </remarks>
		/// <returns>contents of this revision of the document.</returns>
		public abstract IDictionary<string, object> GetProperties();

		/// <summary>Shorthand for getProperties().get(key)</summary>
		public virtual object GetProperty(string key)
		{
			return GetProperties().Get(key);
		}

		/// <summary>The user-defined properties, without the ones reserved by CouchDB.</summary>
		/// <remarks>
		/// The user-defined properties, without the ones reserved by CouchDB.
		/// This is based on -properties, with every key whose name starts with "_" removed.
		/// </remarks>
		/// <returns>user-defined properties, without the ones reserved by CouchDB.</returns>
		public virtual IDictionary<string, object> GetUserProperties()
		{
			IDictionary<string, object> result = new Dictionary<string, object>();
			IDictionary<string, object> sourceMap = GetProperties();
			foreach (string key in sourceMap.Keys)
			{
				if (!key.StartsWith("_"))
				{
					result.Put(key, sourceMap.Get(key));
				}
			}
			return Sharpen.Collections.UnmodifiableMap(result);
		}

		/// <summary>The names of all attachments</summary>
		/// <returns></returns>
		public virtual IList<string> GetAttachmentNames()
		{
			IDictionary<string, object> attachmentMetadata = GetAttachmentMetadata();
			AList<string> result = new AList<string>();
			Sharpen.Collections.AddAll(result, attachmentMetadata.Keys);
			return result;
		}

		/// <summary>Looks up the attachment with the given name (without fetching its contents yet).
		/// 	</summary>
		/// <remarks>Looks up the attachment with the given name (without fetching its contents yet).
		/// 	</remarks>
		/// <param name="name"></param>
		/// <returns></returns>
		public virtual CBLAttachment GetAttachment(string name)
		{
			IDictionary<string, object> attachmentMetadata = GetAttachmentMetadata();
			if (attachmentMetadata == null)
			{
				return null;
			}
			return new CBLAttachment(this, name, attachmentMetadata);
		}

		/// <summary>All attachments, as CBLAttachment objects.</summary>
		/// <remarks>All attachments, as CBLAttachment objects.</remarks>
		/// <returns></returns>
		public virtual IList<CBLAttachment> GetAttachments()
		{
			IList<CBLAttachment> result = new AList<CBLAttachment>();
			IList<string> attachmentNames = GetAttachmentNames();
			foreach (string attachmentName in attachmentNames)
			{
				result.AddItem(GetAttachment(attachmentName));
			}
			return result;
		}

		internal virtual IDictionary<string, object> GetAttachmentMetadata()
		{
			return (IDictionary<string, object>)GetProperty("_attachments");
		}

		public override bool Equals(object o)
		{
			bool result = false;
			if (o is CBLRevision)
			{
				CBLRevision other = (CBLRevision)o;
				if (document.GetId().Equals(other.GetDocument().GetId()) && revId.Equals(other.revId
					))
				{
					result = true;
				}
			}
			return result;
		}

		public override int GetHashCode()
		{
			return document.GetId().GetHashCode() ^ revId.GetHashCode();
		}

		public virtual string GetId()
		{
			return revId;
		}

		internal virtual void SetId(string revId)
		{
			this.revId = revId;
		}

		/// <summary>
		/// Does this revision mark the deletion of its document?
		/// (In other words, does it have a "_deleted" property?)
		/// </summary>
		internal virtual bool IsDeleted()
		{
			object deleted = GetProperty("_deleted");
			if (deleted == null)
			{
				return false;
			}
			bool deletedBool = (bool)deleted;
			return deletedBool;
		}

		internal virtual void SetSequence(long sequence)
		{
			this.sequence = sequence;
		}

		internal virtual long GetSequence()
		{
			return sequence;
		}

		public override string ToString()
		{
			return "{" + this.document.GetId() + " #" + this.revId + (IsDeleted() ? "DEL" : string.Empty
				) + "}";
		}

		/// <summary>Generation number: 1 for a new document, 2 for the 2nd revision, ...</summary>
		/// <remarks>
		/// Generation number: 1 for a new document, 2 for the 2nd revision, ...
		/// Extracted from the numeric prefix of the revID.
		/// </remarks>
		internal virtual int GetGeneration()
		{
			return GenerationFromRevID(revId);
		}

		internal static int GenerationFromRevID(string revID)
		{
			int generation = 0;
			int dashPos = revID.IndexOf("-");
			if (dashPos > 0)
			{
				generation = System.Convert.ToInt32(Sharpen.Runtime.Substring(revID, 0, dashPos));
			}
			return generation;
		}
	}
}
