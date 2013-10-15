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
using Couchbase.Internal;
using Sharpen;

namespace Couchbase
{
	/// <summary>An ordered list of TDRevisions</summary>
	[System.Serializable]
	public class CBLRevisionList : AList<CBLRevisionInternal>
	{
		public CBLRevisionList() : base()
		{
		}

		/// <summary>Allow converting to CBLRevisionList from List<CBLRevisionInternal></summary>
		/// <param name="list"></param>
		public CBLRevisionList(IList<CBLRevisionInternal> list) : base(list)
		{
		}

		public virtual CBLRevisionInternal RevWithDocIdAndRevId(string docId, string revId
			)
		{
			IEnumerator<CBLRevisionInternal> iterator = GetEnumerator();
			while (iterator.HasNext())
			{
				CBLRevisionInternal rev = iterator.Next();
				if (docId.Equals(rev.GetDocId()) && revId.Equals(rev.GetRevId()))
				{
					return rev;
				}
			}
			return null;
		}

		public virtual IList<string> GetAllDocIds()
		{
			IList<string> result = new AList<string>();
			IEnumerator<CBLRevisionInternal> iterator = GetEnumerator();
			while (iterator.HasNext())
			{
				CBLRevisionInternal rev = iterator.Next();
				result.AddItem(rev.GetDocId());
			}
			return result;
		}

		public virtual IList<string> GetAllRevIds()
		{
			IList<string> result = new AList<string>();
			IEnumerator<CBLRevisionInternal> iterator = GetEnumerator();
			while (iterator.HasNext())
			{
				CBLRevisionInternal rev = iterator.Next();
				result.AddItem(rev.GetRevId());
			}
			return result;
		}

		public virtual void SortBySequence()
		{
			this.Sort(new _IComparer_82());
		}

		private sealed class _IComparer_82 : IComparer<CBLRevisionInternal>
		{
			public _IComparer_82()
			{
			}

			public int Compare(CBLRevisionInternal rev1, CBLRevisionInternal rev2)
			{
				return CBLMisc.TDSequenceCompare(rev1.GetSequence(), rev2.GetSequence());
			}
		}

		public virtual void Limit(int limit)
		{
			if (Count > limit)
			{
				RemoveRange(limit, Count);
			}
		}
	}
}
