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
	/// <summary>Enumerator on a CBLQuery's result rows.</summary>
	/// <remarks>
	/// Enumerator on a CBLQuery's result rows.
	/// The objects returned are instances of CBLQueryRow.
	/// </remarks>
	public class CBLQueryEnumerator
	{
		private CBLDatabase database;

		private IList<CBLQueryRow> rows;

		private int nextRow;

		private long sequenceNumber;

		private CBLStatus status;

		internal CBLQueryEnumerator(CBLDatabase database, IList<CBLQueryRow> rows, long sequenceNumber
			)
		{
			this.database = database;
			this.rows = rows;
			this.sequenceNumber = sequenceNumber;
			// Fill in the rows' database reference now
			foreach (CBLQueryRow row in rows)
			{
				row.SetDatabase(database);
			}
		}

		internal CBLQueryEnumerator(CBLDatabase database, CBLStatus status)
		{
			this.database = database;
			this.status = status;
		}

		internal CBLQueryEnumerator(Couchbase.CBLQueryEnumerator other)
		{
			this.database = other.database;
			this.rows = other.rows;
			this.sequenceNumber = other.sequenceNumber;
		}

		public override bool Equals(object o)
		{
			if (this == o)
			{
				return true;
			}
			if (o == null || GetType() != o.GetType())
			{
				return false;
			}
			Couchbase.CBLQueryEnumerator that = (Couchbase.CBLQueryEnumerator)o;
			if (rows != null ? !rows.Equals(that.rows) : that.rows != null)
			{
				return false;
			}
			return true;
		}

		public virtual CBLQueryRow GetRowAtIndex(int index)
		{
			return rows[index];
		}

		public virtual int GetCount()
		{
			return rows.Count;
		}

		public virtual CBLQueryRow GetNextRow()
		{
			if (nextRow >= rows.Count)
			{
				return null;
			}
			return rows[nextRow++];
		}

		/// <summary>TODO: in the spec this is called getError() and returns an "Error" object.
		/// 	</summary>
		/// <remarks>TODO: in the spec this is called getError() and returns an "Error" object.
		/// 	</remarks>
		/// <returns></returns>
		public virtual CBLStatus GetStatus()
		{
			return status;
		}

		public virtual long GetSequenceNumber()
		{
			return sequenceNumber;
		}
	}
}
