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
using System.Collections.Generic;
using Couchbase;
using Couchbase.Internal;
using Sharpen;

namespace Couchbase.Internal
{
	/// <summary>Stores information about a revision -- its docID, revID, and whether it's deleted.
	/// 	</summary>
	/// <remarks>
	/// Stores information about a revision -- its docID, revID, and whether it's deleted.
	/// It can also store the sequence number and document contents (they can be added after creation).
	/// </remarks>
	public class CBLRevisionInternal
	{
		private string docId;

		private string revId;

		private bool deleted;

		private CBLBody body;

		private long sequence;

		private CBLDatabase database;

		public CBLRevisionInternal(string docId, string revId, bool deleted, CBLDatabase 
			database)
		{
			// TODO: get rid of this field!
			this.docId = docId;
			this.revId = revId;
			this.deleted = deleted;
			this.database = database;
		}

		public CBLRevisionInternal(IDictionary<string, object> properties)
		{
			this.body = new CBLBody(properties);
		}

		public CBLRevisionInternal(CBLBody body, CBLDatabase database) : this((string)body
			.GetPropertyForKey("_id"), (string)body.GetPropertyForKey("_rev"), (((bool)body.
			GetPropertyForKey("_deleted") != null) && ((bool)body.GetPropertyForKey("_deleted"
			) == true)), database)
		{
			this.body = body;
		}

		public CBLRevisionInternal(IDictionary<string, object> properties, CBLDatabase database
			) : this(new CBLBody(properties), database)
		{
		}

		public virtual IDictionary<string, object> GetProperties()
		{
			IDictionary<string, object> result = null;
			if (body != null)
			{
				result = body.GetProperties();
			}
			return result;
		}

		public virtual object GetPropertyForKey(string key)
		{
			return GetProperties().Get(key);
		}

		public virtual void SetProperties(IDictionary<string, object> properties)
		{
			this.body = new CBLBody(properties);
		}

		public virtual byte[] GetJson()
		{
			byte[] result = null;
			if (body != null)
			{
				result = body.GetJson();
			}
			return result;
		}

		public virtual void SetJson(byte[] json)
		{
			this.body = new CBLBody(json);
		}

		public override bool Equals(object o)
		{
			bool result = false;
			if (o is Couchbase.Internal.CBLRevisionInternal)
			{
				Couchbase.Internal.CBLRevisionInternal other = (Couchbase.Internal.CBLRevisionInternal
					)o;
				if (docId.Equals(other.docId) && revId.Equals(other.revId))
				{
					result = true;
				}
			}
			return result;
		}

		public override int GetHashCode()
		{
			return docId.GetHashCode() ^ revId.GetHashCode();
		}

		public virtual string GetDocId()
		{
			return docId;
		}

		public virtual void SetDocId(string docId)
		{
			this.docId = docId;
		}

		public virtual string GetRevId()
		{
			return revId;
		}

		public virtual void SetRevId(string revId)
		{
			this.revId = revId;
		}

		public virtual bool IsDeleted()
		{
			return deleted;
		}

		public virtual void SetDeleted(bool deleted)
		{
			this.deleted = deleted;
		}

		public virtual CBLBody GetBody()
		{
			return body;
		}

		public virtual void SetBody(CBLBody body)
		{
			this.body = body;
		}

		public virtual Couchbase.Internal.CBLRevisionInternal CopyWithDocID(string docId, 
			string revId)
		{
			System.Diagnostics.Debug.Assert(((docId != null) && (revId != null)));
			System.Diagnostics.Debug.Assert(((this.docId == null) || (this.docId.Equals(docId
				))));
			Couchbase.Internal.CBLRevisionInternal result = new Couchbase.Internal.CBLRevisionInternal
				(docId, revId, deleted, database);
			IDictionary<string, object> properties = GetProperties();
			if (properties == null)
			{
				properties = new Dictionary<string, object>();
			}
			properties.Put("_id", docId);
			properties.Put("_rev", revId);
			result.SetProperties(properties);
			return result;
		}

		public virtual void SetSequence(long sequence)
		{
			this.sequence = sequence;
		}

		public virtual long GetSequence()
		{
			return sequence;
		}

		public override string ToString()
		{
			return "{" + this.docId + " #" + this.revId + (deleted ? "DEL" : string.Empty) + 
				"}";
		}

		/// <summary>Generation number: 1 for a new document, 2 for the 2nd revision, ...</summary>
		/// <remarks>
		/// Generation number: 1 for a new document, 2 for the 2nd revision, ...
		/// Extracted from the numeric prefix of the revID.
		/// </remarks>
		public virtual int GetGeneration()
		{
			return GenerationFromRevID(revId);
		}

		public static int GenerationFromRevID(string revID)
		{
			int generation = 0;
			int dashPos = revID.IndexOf("-");
			if (dashPos > 0)
			{
				generation = System.Convert.ToInt32(Sharpen.Runtime.Substring(revID, 0, dashPos));
			}
			return generation;
		}

		public static int CBLCollateRevIDs(string revId1, string revId2)
		{
			string rev1GenerationStr = null;
			string rev2GenerationStr = null;
			string rev1Hash = null;
			string rev2Hash = null;
			StringTokenizer st1 = new StringTokenizer(revId1, "-");
			try
			{
				rev1GenerationStr = st1.NextToken();
				rev1Hash = st1.NextToken();
			}
			catch (Exception)
			{
			}
			StringTokenizer st2 = new StringTokenizer(revId2, "-");
			try
			{
				rev2GenerationStr = st2.NextToken();
				rev2Hash = st2.NextToken();
			}
			catch (Exception)
			{
			}
			// improper rev IDs; just compare as plain text:
			if (rev1GenerationStr == null || rev2GenerationStr == null)
			{
				return revId1.CompareToIgnoreCase(revId2);
			}
			int rev1Generation;
			int rev2Generation;
			try
			{
				rev1Generation = System.Convert.ToInt32(rev1GenerationStr);
				rev2Generation = System.Convert.ToInt32(rev2GenerationStr);
			}
			catch (FormatException)
			{
				// improper rev IDs; just compare as plain text:
				return revId1.CompareToIgnoreCase(revId2);
			}
			// Compare generation numbers; if they match, compare suffixes:
			if (rev1Generation.CompareTo(rev2Generation) != 0)
			{
				return rev1Generation.CompareTo(rev2Generation);
			}
			else
			{
				if (rev1Hash != null && rev2Hash != null)
				{
					// compare suffixes if possible
					return Sharpen.Runtime.CompareOrdinal(rev1Hash, rev2Hash);
				}
				else
				{
					// just compare as plain text:
					return revId1.CompareToIgnoreCase(revId2);
				}
			}
		}

		public static int CBLCompareRevIDs(string revId1, string revId2)
		{
			System.Diagnostics.Debug.Assert((revId1 != null));
			System.Diagnostics.Debug.Assert((revId2 != null));
			return CBLCollateRevIDs(revId1, revId2);
		}
	}
}
