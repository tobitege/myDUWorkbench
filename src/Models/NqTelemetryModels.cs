using System;
using System.Globalization;

namespace myDUWorkbench.Models;

public readonly record struct Vec3(double X, double Y, double Z)
{
    public double Magnitude => Math.Sqrt((X * X) + (Y * Y) + (Z * Z));

    public override string ToString()
    {
        return string.Create(
            CultureInfo.InvariantCulture,
            $"{X:R}, {Y:R}, {Z:R}");
    }

    public static Vec3 Deserialize(NqBinaryDeserializer deser)
    {
        return new Vec3(
            deser.ReadDouble(),
            deser.ReadDouble(),
            deser.ReadDouble());
    }

    public static bool TryParseCsv(string? value, out Vec3 vec)
    {
        vec = default;
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        string[] parts = value.Split(',');
        if (parts.Length != 3)
        {
            return false;
        }

        if (!double.TryParse(parts[0], NumberStyles.Float, CultureInfo.InvariantCulture, out double x))
        {
            return false;
        }

        if (!double.TryParse(parts[1], NumberStyles.Float, CultureInfo.InvariantCulture, out double y))
        {
            return false;
        }

        if (!double.TryParse(parts[2], NumberStyles.Float, CultureInfo.InvariantCulture, out double z))
        {
            return false;
        }

        vec = new Vec3(x, y, z);
        return true;
    }
}

public readonly record struct Quat(float X, float Y, float Z, float W)
{
    public override string ToString()
    {
        return string.Create(
            CultureInfo.InvariantCulture,
            $"{W:R}, {X:R}, {Y:R}, {Z:R}");
    }

    public static Quat Deserialize(NqBinaryDeserializer deser)
    {
        return new Quat(
            deser.ReadFloat(),
            deser.ReadFloat(),
            deser.ReadFloat(),
            deser.ReadFloat());
    }

    public static bool TryParseCsv(string? value, out Quat quat)
    {
        quat = default;
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        string[] parts = value.Split(',');
        if (parts.Length != 4)
        {
            return false;
        }

        if (!float.TryParse(parts[0], NumberStyles.Float, CultureInfo.InvariantCulture, out float w))
        {
            return false;
        }

        if (!float.TryParse(parts[1], NumberStyles.Float, CultureInfo.InvariantCulture, out float x))
        {
            return false;
        }

        if (!float.TryParse(parts[2], NumberStyles.Float, CultureInfo.InvariantCulture, out float y))
        {
            return false;
        }

        if (!float.TryParse(parts[3], NumberStyles.Float, CultureInfo.InvariantCulture, out float z))
        {
            return false;
        }

        quat = new Quat(x, y, z, w);
        return true;
    }
}

public sealed record ConstructUpdate(
    ulong ConstructId,
    ulong BaseId,
    Vec3 Position,
    Quat Rotation,
    Vec3 WorldRelativeVelocity,
    Vec3 WorldAbsoluteVelocity,
    Vec3 WorldRelativeAngularVelocity,
    Vec3 WorldAbsoluteAngularVelocity,
    ulong PilotId,
    bool Grounded,
    long NetworkTime)
{
    public static ConstructUpdate Deserialize(NqBinaryDeserializer deser)
    {
        return new ConstructUpdate(
            deser.ReadVaruint(),
            deser.ReadVaruint(),
            Vec3.Deserialize(deser),
            Quat.Deserialize(deser),
            Vec3.Deserialize(deser),
            Vec3.Deserialize(deser),
            Vec3.Deserialize(deser),
            Vec3.Deserialize(deser),
            deser.ReadVaruint(),
            deser.ReadU8() > 0,
            deser.ReadVarint());
    }
}

public sealed record ConstructInfoPreamble(
    ulong ConstructId,
    ulong ParentId,
    Vec3 Position,
    Quat Rotation)
{
    public static ConstructInfoPreamble Deserialize(NqBinaryDeserializer deser)
    {
        return new ConstructInfoPreamble(
            deser.ReadVaruint(),
            deser.ReadVaruint(),
            Vec3.Deserialize(deser),
            Quat.Deserialize(deser));
    }
}

public sealed record NqStructBlobHeader(
    long Timestamp,
    ulong Target,
    long MessageType,
    long Format,
    int PayloadLength)
{
    public static NqStructBlobHeader DeserializeHeader(NqBinaryDeserializer deser)
    {
        long timestamp = deser.ReadVarint();
        ulong target = deser.ReadVaruint();
        long messageType = deser.ReadVarint();
        long format = deser.ReadVarint();
        byte[] payload = deser.ReadBuffer();
        return new NqStructBlobHeader(timestamp, target, messageType, format, payload.Length);
    }
}
