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
	/// <summary>Stores information about a revision -- its docID, revID, and whether it's deleted.
	/// 	</summary>
	/// <remarks>
	/// Stores information about a revision -- its docID, revID, and whether it's deleted.
	/// It can also store the sequence number and document contents (they can be added after creation).
	/// </remarks>
	public class CBLRevision : CBLRevisionBase
	{
		private CBLRevisionInternal revisionInternal;

		private bool checkedProperties;

		internal CBLRevision(CBLDocument document, CBLRevisionInternal revision) : base(document
			)
		{
			this.revisionInternal = revision;
		}

		internal CBLRevision(CBLDatabase database, CBLRevisionInternal revision) : this(database
			.GetDocument(revision.GetDocId()), revision)
		{
		}

		/// <summary>
		/// Creates a new mutable child revision whose properties and attachments are initially identical
		/// to this one's, which you can modify and then save.
		/// </summary>
		/// <remarks>
		/// Creates a new mutable child revision whose properties and attachments are initially identical
		/// to this one's, which you can modify and then save.
		/// </remarks>
		/// <returns></returns>
		public virtual CBLNewRevision CreateNewRevision()
		{
			CBLNewRevision newRevision = new CBLNewRevision(document, this);
			return newRevision;
		}

		/// <summary>The contents of this revision of the document.</summary>
		/// <remarks>
		/// The contents of this revision of the document.
		/// Any keys in the dictionary that begin with "_", such as "_id" and "_rev", contain CouchbaseLite metadata.
		/// </remarks>
		/// <returns>contents of this revision of the document.</returns>
		public override IDictionary<string, object> GetProperties()
		{
			IDictionary<string, object> properties = revisionInternal.GetProperties();
			if (properties == null && !checkedProperties)
			{
				if (LoadProperties() == true)
				{
					properties = revisionInternal.GetProperties();
				}
				checkedProperties = true;
			}
			return Sharpen.Collections.UnmodifiableMap(properties);
		}

		internal virtual bool LoadProperties()
		{
			try
			{
				Dictionary<string, object> emptyProperties = new Dictionary<string, object>();
				CBLRevisionInternal loadRevision = new CBLRevisionInternal(emptyProperties, database
					);
				database.LoadRevisionBody(loadRevision, EnumSet.NoneOf<CBLDatabase.TDContentOptions
					>());
				if (loadRevision == null)
				{
					Log.W(CBLDatabase.Tag, "Couldn't load body/sequence of %s" + this);
					return false;
				}
				revisionInternal = loadRevision;
				return true;
			}
			catch (CBLiteException e)
			{
				throw new RuntimeException(e);
			}
		}

		/// <summary>Creates and saves a new revision with the given properties.</summary>
		/// <remarks>
		/// Creates and saves a new revision with the given properties.
		/// This will fail with a 412 error if the receiver is not the current revision of the document.
		/// </remarks>
		/// <exception cref="Couchbase.CBLiteException"></exception>
		public virtual Couchbase.CBLRevision PutProperties(IDictionary<string, object> properties
			)
		{
			return document.PutProperties(properties, revisionInternal.GetRevId());
		}

		/// <summary>Has this object fetched its contents from the database yet?</summary>
		public virtual bool IsPropertiesLoaded()
		{
			return revisionInternal.GetProperties() != null;
		}

		/// <summary>Deletes the document by creating a new deletion-marker revision.</summary>
		/// <remarks>Deletes the document by creating a new deletion-marker revision.</remarks>
		/// <returns></returns>
		/// <exception cref="CBLiteException">CBLiteException</exception>
		/// <exception cref="Couchbase.CBLiteException"></exception>
		public virtual Couchbase.CBLRevision DeleteDocument()
		{
			return PutProperties(null);
		}

		/// <summary>Returns the history of this document as an array of CBLRevisions, in forward order.
		/// 	</summary>
		/// <remarks>
		/// Returns the history of this document as an array of CBLRevisions, in forward order.
		/// Older revisions are NOT guaranteed to have their properties available.
		/// </remarks>
		/// <returns></returns>
		/// <exception cref="CBLiteException">CBLiteException</exception>
		/// <exception cref="Couchbase.CBLiteException"></exception>
		public virtual IList<Couchbase.CBLRevision> GetRevisionHistory()
		{
			IList<Couchbase.CBLRevision> revisions = new AList<Couchbase.CBLRevision>();
			IList<CBLRevisionInternal> internalRevisions = database.GetRevisionHistory(revisionInternal
				);
			foreach (CBLRevisionInternal internalRevision in internalRevisions)
			{
				if (internalRevision.GetRevId().Equals(GetId()))
				{
					revisions.AddItem(this);
				}
				else
				{
					Couchbase.CBLRevision revision = document.GetRevisionFromRev(internalRevision);
					revisions.AddItem(revision);
				}
			}
			Sharpen.Collections.Reverse(revisions);
			return Sharpen.Collections.UnmodifiableList(revisions);
		}
	}
}
