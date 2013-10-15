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

using System.Collections;
using System.Collections.Generic;
using Couchbase;
using Sharpen;

namespace Couchbase
{
	/// <summary>A result row from a CouchbaseLite view query.</summary>
	/// <remarks>
	/// A result row from a CouchbaseLite view query.
	/// Full-text and geo queries return subclasses -- see CBLFullTextQueryRow and CBLGeoQueryRow.
	/// </remarks>
	public class CBLQueryRow
	{
		private object key;

		private object value;

		private long sequence;

		private string sourceDocumentId;

		private IDictionary<string, object> documentProperties;

		private CBLDatabase database;

		/// <summary>
		/// Constructor
		/// The database property will be filled in when I'm added to a CBLQueryEnumerator.
		/// </summary>
		/// <remarks>
		/// Constructor
		/// The database property will be filled in when I'm added to a CBLQueryEnumerator.
		/// </remarks>
		internal CBLQueryRow(string documentId, long sequence, object key, object value, 
			IDictionary<string, object> documentProperties)
		{
			this.sourceDocumentId = documentId;
			this.sequence = sequence;
			this.key = key;
			this.value = value;
			this.documentProperties = documentProperties;
		}

		/// <summary>
		/// This is used implicitly by -[CBLLiveQuery update] to decide whether the query result has changed
		/// enough to notify the client.
		/// </summary>
		/// <remarks>
		/// This is used implicitly by -[CBLLiveQuery update] to decide whether the query result has changed
		/// enough to notify the client. So it's important that it not give false positives, else the app
		/// won't get notified of changes.
		/// </remarks>
		public override bool Equals(object @object)
		{
			if (@object == this)
			{
				return true;
			}
			if (!(@object is Couchbase.CBLQueryRow))
			{
				return false;
			}
			Couchbase.CBLQueryRow other = (Couchbase.CBLQueryRow)@object;
			if (database == other.database && key.Equals(other.GetKey()) && sourceDocumentId.
				Equals(other.GetSourceDocumentId()) && documentProperties.Equals(other.GetDocumentProperties
				()))
			{
				// If values were emitted, compare them. Otherwise we have nothing to go on so check
				// if _anything_ about the doc has changed (i.e. the sequences are different.)
				if (value != null || other.GetValue() != null)
				{
					return value.Equals(other.GetValue());
				}
				else
				{
					return sequence == other.sequence;
				}
			}
			return false;
		}

		/// <summary>The row's key: this is the first parameter passed to the emit() call that generated the row.
		/// 	</summary>
		/// <remarks>The row's key: this is the first parameter passed to the emit() call that generated the row.
		/// 	</remarks>
		public virtual object GetKey()
		{
			return key;
		}

		/// <summary>The row's value: this is the second parameter passed to the emit() call that generated the row.
		/// 	</summary>
		/// <remarks>The row's value: this is the second parameter passed to the emit() call that generated the row.
		/// 	</remarks>
		public virtual object GetValue()
		{
			return value;
		}

		/// <summary>The ID of the document described by this view row.</summary>
		/// <remarks>
		/// The ID of the document described by this view row.  This is not necessarily the same as the
		/// document that caused this row to be emitted; see the discussion of the .sourceDocumentID
		/// property for details.
		/// </remarks>
		public virtual string GetDocumentId()
		{
			// _documentProperties may have been 'redirected' from a different document
			object idFromDocumentProperties = documentProperties.Get("_id");
			if (idFromDocumentProperties != null && (idFromDocumentProperties is string))
			{
				return (string)idFromDocumentProperties;
			}
			else
			{
				return sourceDocumentId;
			}
		}

		/// <summary>The ID of the document that caused this view row to be emitted.</summary>
		/// <remarks>
		/// The ID of the document that caused this view row to be emitted.  This is the value of
		/// the "id" property of the JSON view row. It will be the same as the .documentID property,
		/// unless the map function caused a related document to be linked by adding an "_id" key to
		/// the emitted value; in this case .documentID will refer to the linked document, while
		/// sourceDocumentID always refers to the original document.  In a reduced or grouped query
		/// the value will be nil, since the rows don't correspond to individual documents.
		/// </remarks>
		public virtual string GetSourceDocumentId()
		{
			return sourceDocumentId;
		}

		/// <summary>The revision ID of the document this row was mapped from.</summary>
		/// <remarks>The revision ID of the document this row was mapped from.</remarks>
		public virtual string GetDocumentRevisionId()
		{
			string rev = (string)documentProperties.Get("_rev");
			if (rev == null)
			{
				if (value is IDictionary)
				{
					IDictionary<string, object> mapValue = (IDictionary<string, object>)value;
					rev = (string)mapValue.Get("_rev");
					if (rev == null)
					{
						rev = (string)mapValue.Get("rev");
					}
				}
			}
			return rev;
		}

		public virtual CBLDatabase GetDatabase()
		{
			return database;
		}

		/// <summary>The document this row was mapped from.</summary>
		/// <remarks>
		/// The document this row was mapped from.  This will be nil if a grouping was enabled in
		/// the query, because then the result rows don't correspond to individual documents.
		/// </remarks>
		public virtual CBLDocument GetDocument()
		{
			if (GetDocumentId() == null)
			{
				return null;
			}
			CBLDocument document = database.GetDocument(GetDocumentId());
			document.LoadCurrentRevisionFrom(this);
			return document;
		}

		/// <summary>The properties of the document this row was mapped from.</summary>
		/// <remarks>
		/// The properties of the document this row was mapped from.
		/// To get this, you must have set the -prefetch property on the query; else this will be nil.
		/// </remarks>
		public virtual IDictionary<string, object> GetDocumentProperties()
		{
			return Sharpen.Collections.UnmodifiableMap(documentProperties);
		}

		/// <summary>If this row's key is an array, returns the item at that index in the array.
		/// 	</summary>
		/// <remarks>
		/// If this row's key is an array, returns the item at that index in the array.
		/// If the key is not an array, index=0 will return the key itself.
		/// If the index is out of range, returns nil.
		/// </remarks>
		public virtual object GetKeyAtIndex(int index)
		{
			if (key is IList)
			{
				IList keyList = (IList)key;
				if (index < keyList.Count)
				{
					return keyList[index];
				}
			}
			else
			{
				if (index == 0)
				{
					return key;
				}
			}
			return null;
		}

		/// <summary>Convenience for use in keypaths.</summary>
		/// <remarks>Convenience for use in keypaths. Returns the key at the given index.</remarks>
		public virtual object GetKey0()
		{
			return GetKeyAtIndex(0);
		}

		public virtual object GetKey1()
		{
			return GetKeyAtIndex(1);
		}

		public virtual object GetKey2()
		{
			return GetKeyAtIndex(2);
		}

		public virtual object GetKey3()
		{
			return GetKeyAtIndex(3);
		}

		/// <summary>The local sequence number of the associated doc/revision.</summary>
		/// <remarks>The local sequence number of the associated doc/revision.</remarks>
		public virtual long GetLocalSequence()
		{
			return sequence;
		}

		internal virtual void SetDatabase(CBLDatabase database)
		{
			this.database = database;
		}

		public virtual IDictionary<string, object> AsJSONDictionary()
		{
			IDictionary<string, object> result = new Dictionary<string, object>();
			if (value != null || sourceDocumentId != null)
			{
				result.Put("key", key);
				result.Put("value", value);
				result.Put("id", sourceDocumentId);
				result.Put("doc", documentProperties);
			}
			else
			{
				result.Put("key", key);
				result.Put("error", "not_found");
			}
			return result;
		}
	}
}
