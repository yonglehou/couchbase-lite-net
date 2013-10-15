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
	public class CBLNewRevision : CBLRevisionBase
	{
		private string parentRevID;

		private IDictionary<string, object> properties;

		protected internal CBLNewRevision(CBLDocument document, CBLRevision parentRevision
			) : base(document)
		{
			// private CBLBody body;
			parentRevID = parentRevision.GetId();
			IDictionary<string, object> parentRevisionProperties = parentRevision.GetProperties
				();
			if (parentRevisionProperties == null)
			{
				properties = new Dictionary<string, object>();
				properties.Put("_id", document.GetId());
				properties.Put("_rev", parentRevID);
			}
			else
			{
				properties = new Dictionary<string, object>(parentRevisionProperties);
			}
		}

		public virtual void SetProperties(IDictionary<string, object> properties)
		{
			this.properties = properties;
		}

		public override IDictionary<string, object> GetProperties()
		{
			return properties;
		}

		public virtual void SetDeleted(bool deleted)
		{
			if (deleted == true)
			{
				properties.Put("_deleted", true);
			}
			else
			{
				Sharpen.Collections.Remove(properties, "_deleted");
			}
		}

		public virtual CBLRevision GetParentRevision()
		{
			if (parentRevID == null || parentRevID.Length == 0)
			{
				return null;
			}
			return document.GetRevision(parentRevID);
		}

		public virtual string GetParentRevisionId()
		{
			return parentRevID;
		}

		/// <exception cref="Couchbase.CBLiteException"></exception>
		public virtual CBLRevision Save()
		{
			return document.PutProperties(properties, parentRevID);
		}

		public virtual void AddAttachment(CBLAttachment attachment, string name)
		{
			IDictionary<string, object> attachments = (IDictionary<string, object>)properties
				.Get("_attachments");
			attachments.Put(name, attachment);
			properties.Put("_attachments", attachments);
			attachment.SetName(name);
			attachment.SetRevision(this);
		}

		public virtual void RemoveAttachmentNamed(string name)
		{
			AddAttachment(null, name);
		}
	}
}
