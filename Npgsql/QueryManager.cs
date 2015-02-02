using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;

namespace Npgsql
{
    /// <summary>
    /// Responsible for creating query messages and writing them to the wire.
    /// 
    /// Contains some frequently-used queries as buffers ready to be sent.
    /// </summary>
    internal static class QueryManager
    {
        internal static void WriteQuery(NpgsqlBuffer buffer, string query)
        {
            var strlen = BackendEncoding.UTF8Encoding.GetByteCount(query);
            var len = 4 + strlen + 1;
            buffer
                .EnsureWrite(5)
                .WriteByte((byte)FrontendMessageCode.Query)
                .WriteInt32(len)
                .WriteString(query, len)
                .EnsuredWriteByte(0)
                .Flush();
        }

        internal static void WriteQuery(Stream stream, string query)
        {
            var bytes = BackendEncoding.UTF8Encoding.GetBytes(query);
            var len = 4 + bytes.Length + 1;
            stream
                .WriteBytes(ASCIIByteArrays.QueryMessageCode)
                .WriteInt32(len)
                .WriteString(query, len)
                .WriteBytes(ASCIIByteArrays.Byte_0)
                .Flush();
        }

//#if NET45
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
//#endif
        internal static void WriteQuery(NpgsqlBuffer buffer, byte[] query)
        {
            var len = 4 + query.Length;

            buffer
                .EnsureWrite(5)
                .WriteByte((byte)FrontendMessageCode.Query)
                .WriteInt32(len)
                .WriteBytes(query)
                .Flush();
        }

        static QueryManager()
        {
            BeginTransRepeatableRead = BuildQuery("BEGIN; SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;");
            BeginTransSerializable   = BuildQuery("BEGIN; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;");
            BeginTransReadCommitted  = BuildQuery("BEGIN; SET TRANSACTION ISOLATION LEVEL READ COMMITTED;");
            CommitTransaction        = BuildQuery("COMMIT");
            RollbackTransaction      = BuildQuery("ROLLBACK");
            DiscardAll               = BuildQuery("DISCARD ALL");
            UnlistenAll              = BuildQuery("UNLISTEN *");
            SetStmtTimeout10Sec      = BuildQuery("SET statement_timeout = 10000");
            SetStmtTimeout20Sec      = BuildQuery("SET statement_timeout = 20000");
            SetStmtTimeout30Sec      = BuildQuery("SET statement_timeout = 30000");
            SetStmtTimeout60Sec      = BuildQuery("SET statement_timeout = 60000");
            SetStmtTimeout90Sec      = BuildQuery("SET statement_timeout = 90000");
            SetStmtTimeout120Sec     = BuildQuery("SET statement_timeout = 120000");
        }

        static byte[] BuildQuery(string query)
        {
            var ms = new MemoryStream();
            WriteQuery(ms, query);
            return ms.ToArray();
        }

        internal static readonly byte[] BeginTransRepeatableRead;
        internal static readonly byte[] BeginTransSerializable;
        internal static readonly byte[] BeginTransReadCommitted;
        internal static readonly byte[] CommitTransaction;
        internal static readonly byte[] RollbackTransaction;
        internal static readonly byte[] DiscardAll;
        internal static readonly byte[] UnlistenAll;
        internal static readonly byte[] SetStmtTimeout10Sec;
        internal static readonly byte[] SetStmtTimeout20Sec;
        internal static readonly byte[] SetStmtTimeout30Sec;
        internal static readonly byte[] SetStmtTimeout60Sec;
        internal static readonly byte[] SetStmtTimeout90Sec;
        internal static readonly byte[] SetStmtTimeout120Sec;
    }
}
