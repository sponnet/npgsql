﻿using Npgsql.Messages;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NpgsqlTypes;
using System.Diagnostics.Contracts;

namespace Npgsql.TypeHandlers
{
    internal class RangeHandler<T> : TypeHandler<NpgsqlRange<T>>, IChunkingTypeReader<NpgsqlRange<T>>, IChunkingTypeWriter
    {
        public override bool SupportsBinaryWrite { get { return ElementHandler.SupportsBinaryWrite; } }

        const int Empty = 1;
        const int LbInc = 2;
        const int UbInc = 4;
        const int LbInf = 8;
        const int UbInf = 16;

        internal override Type GetFieldType(FieldDescription fieldDescription)
        {
            return typeof(NpgsqlRange<T>);
        }

        internal override Type GetProviderSpecificFieldType(FieldDescription fieldDescription)
        {
            return typeof(NpgsqlRange<T>); // TODO
        }

        internal Type GetElementFieldType(FieldDescription fieldDescription)
        {
            return typeof(T);
        }
        internal Type GetElementPsvType(FieldDescription fieldDescription)
        {
            return typeof(T); // TODO
        }

        /// <summary>
        /// The type handler for the element that this range type holds
        /// </summary>
        internal TypeHandler<T> ElementHandler { get; private set; }

        public RangeHandler(TypeHandler<T> elementHandler, string name)
        {
            ElementHandler = elementHandler;
            PgName = name;
        }

        public NpgsqlRange<T> Read(NpgsqlBuffer buf, FieldDescription fieldDescription, int len)
        {
            throw new NotImplementedException();
            /*
            Contract.Assert(ElementHandler.SupportsBinaryRead);

            if (IsChunking)
                buf.Ensure(1);

            byte flags = buf.ReadByte();
            T e1 = default(T), e2 = default(T);
            if ((flags & (Empty | LbInf)) == 0) // Is not empty and lower bound is not -inf
            {
                if (IsChunking)
                    buf.Ensure(4);
                var elementLength = buf.ReadInt32();
                e1 = ElementHandler.Read(buf, fieldDescription, elementLength);
            }
            if ((flags & (Empty | UbInf)) == 0) // Is not empty and upper bound is not inf
            {
                if (IsChunking)
                    buf.Ensure(4);
                var elementLength = buf.ReadInt32();
                e2 = ElementHandler.Read(buf, fieldDescription, elementLength);
            }

            if ((flags & Empty) == 1)
                return NpgsqlRange<T>.Empty;
            else
                return new NpgsqlRange<T>(e1, (flags & LbInc) != 0, (flags & LbInf) != 0, e2, (flags & UbInc) != 0, (flags & UbInf) != 0);
             */
        }

        public int ValidateAndGetLength(object value)
        {
            throw new NotImplementedException("Chunking");
            /*
            var range = (NpgsqlRange<T>)value;

            var elementOid = registry.GetElementOidFromRangeOid(oid);

            var sizeArrPos = sizeArr.Count;
            sizeArr.Add(0);

            int totalLen = 1;

            if (!range.IsEmpty && !range.LowerBoundIsMinusInfinity)
            {
                totalLen += ElementHandler.BinarySize(registry, elementOid, range.LowerBound, sizeArr);
            }
            if (!range.IsEmpty && !range.UpperBoundIsInfinity)
            {
                totalLen += ElementHandler.BinarySize(registry, elementOid, range.UpperBound, sizeArr);
            }

            sizeArr[sizeArrPos] = totalLen;

            return totalLen;
             */
        }

        public void PrepareWrite(NpgsqlBuffer buf, object value)
        {
            throw new NotImplementedException();
        }

        public bool Write(ref byte[] directBuf)
        {
            throw new NotImplementedException();
        }

        internal void WriteBinary(object value, NpgsqlBuffer buf)
        {
            throw new NotImplementedException();
            /*
            var range = (NpgsqlRange<T>)value;

            var elementOid = registry.GetElementOidFromRangeOid(oid);

            var totalSize = sizeArr[sizeArrPos++];

            buf.EnsureWrite(CanReadFromSocket ? 4 + 1 : totalSize);

            buf.WriteInt32(totalSize);
            buf.WriteByte((byte)((range.IsEmpty ? Empty : 0) | (range.LowerBoundIsInclusive ? LbInc : 0) | (range.LowerBoundIsMinusInfinity ? LbInf : 0) | (range.UpperBoundIsInclusive ? UbInc : 0) | (range.UpperBoundIsInfinity ? UbInf : 0)));

            if (!range.IsEmpty && !range.LowerBoundIsMinusInfinity)
            {
                ElementHandler.WriteBinary(registry, elementOid, range.LowerBound, buf, sizeArr, ref sizeArrPos);
            }
            if (!range.IsEmpty && !range.UpperBoundIsInfinity)
            {
                ElementHandler.WriteBinary(registry, elementOid, range.UpperBound, buf, sizeArr, ref sizeArrPos);
            }
             */
        }

        public void PrepareRead(NpgsqlBuffer buf, FieldDescription fieldDescription, int len)
        {
            throw new NotImplementedException();
        }

        public bool Read(out NpgsqlRange<T> result)
        {
            throw new NotImplementedException();
        }
    }
}
