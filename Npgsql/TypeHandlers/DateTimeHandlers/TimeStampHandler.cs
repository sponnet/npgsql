using System;
using Npgsql.Messages;
using NpgsqlTypes;
using System.Data;
using System.Diagnostics.Contracts;

namespace Npgsql.TypeHandlers.DateTimeHandlers
{
    /// <remarks>
    /// http://www.postgresql.org/docs/9.3/static/datatype-datetime.html
    /// </remarks>
    [TypeMapping("timestamp", NpgsqlDbType.Timestamp, new[] { DbType.DateTime, DbType.DateTime2 }, typeof(NpgsqlTimeStamp))]
    internal class TimeStampHandler : TypeHandlerWithPsv<DateTime, NpgsqlTimeStamp>, ITypeHandler<NpgsqlTimeStamp>
    {
        public override bool SupportsBinaryWrite
        {
            get
            {
                //return false; // TODO: Implement
                return true;
            }
        }

        public override DateTime Read(NpgsqlBuffer buf, FieldDescription fieldDescription, int len)
        {
            // TODO: Convert directly to DateTime without passing through NpgsqlTimeStamp?
            return (DateTime)((ITypeHandler<NpgsqlTimeStamp>)this).Read(buf, fieldDescription, len);
        }

        NpgsqlTimeStamp ITypeHandler<NpgsqlTimeStamp>.Read(NpgsqlBuffer buf, FieldDescription fieldDescription, int len)
        {
            switch (fieldDescription.FormatCode)
            {
                case FormatCode.Text:
                    return NpgsqlTimeStamp.Parse(buf.ReadString(len));
                case FormatCode.Binary:
                    return NpgsqlTimeStamp.FromInt64(buf.ReadInt64());
                default:
                    throw PGUtil.ThrowIfReached("Unknown format code: " + fieldDescription.FormatCode);
            }
        }

        internal override int Length(object value)
        {
            return 8;
        }

        internal override void WriteBinary(object value, NpgsqlBuffer buf)
        {
            NpgsqlTimeStamp timestamp = new NpgsqlTimeStamp();

            if ( value is DateTime )
            {
                var datumtijd = (DateTime)value;
                var datum = new NpgsqlDate(datumtijd);
                //var tijd = new NpgsqlTime(datumtijd.Hour, datumtijd.Minute, datumtijd.Second, datumtijd.Millisecond);
                var tijd = new NpgsqlTime(datumtijd.Hour, datumtijd.Minute, datumtijd.Second);
                timestamp = new NpgsqlTimeStamp(datum, tijd);
            }
            else if ( value is string )
            {
                timestamp = NpgsqlTimeStamp.Parse((string)value);
            }

            if ( timestamp >= new NpgsqlTimeStamp(2000, 1, 1, 0, 0, 0) )
            {

                var uSecsDate = ( timestamp.Date.DaysSinceEra - 730119 ) * 86400000000L;
                var uSecsTime = timestamp.Time.Hours * 3600000000L + timestamp.Time.Minutes * 60000000 + timestamp.Time.Seconds * 1000000;

                buf.WriteInt64(uSecsDate + uSecsTime);

            }
            else
            {
                // ToDo voor datums voor 2000
            }

            // ToDo : zou beter in DateDataTypes passen : in ToInt64 method

            //var dt = (NpgsqlDate)value;
            //if ( dt == new NpgsqlDate(DateTime.MinValue) )
            //    buf.WriteInt32(int.MinValue);
            //else if ( dt == new NpgsqlDate(DateTime.MaxValue) )
            //    buf.WriteInt32(int.MaxValue);
            //else
            //    buf.WriteInt32(dt.DaysSinceEra - 730119);
        }
    }
}
