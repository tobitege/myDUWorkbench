// Helper Index:
// - EnsureAtEnd: Verifies full payload consumption and reports trailing-byte context.
// - ReadVaruintRaw / ReadVarint: Decodes Novaquark-style varint encodings safely.
// - ReadString / ReadBuffer: Reads length-prefixed UTF-8 strings and byte buffers.
// - EnsureAvailable: Enforces bounds checks before every low-level read operation.
using System;
using System.Text;

namespace myDUWorker.Models;

public sealed class NqBinaryDeserializer
{
    private readonly ReadOnlyMemory<byte> _data;
    private int _offset;

    public NqBinaryDeserializer(ReadOnlyMemory<byte> data)
    {
        _data = data;
        _offset = 0;
    }

    public bool IsAtEnd => _offset >= _data.Length;

    public int Remaining => _data.Length - _offset;

    public void EnsureAtEnd(string? context = null)
    {
        if (!IsAtEnd)
        {
            string prefix = string.IsNullOrWhiteSpace(context)
                ? "Binary payload has trailing unread bytes"
                : $"Binary payload has trailing unread bytes after {context}";
            throw new FormatException($"{prefix}: {Remaining} byte(s) remaining.");
        }
    }

    public byte ReadU8()
    {
        EnsureAvailable(1);
        byte value = _data.Span[_offset];
        _offset++;
        return value;
    }

    public ushort ReadU16()
    {
        byte a = ReadU8();
        byte b = ReadU8();
        return (ushort)(a | (b << 8));
    }

    public short ReadI16() => unchecked((short)ReadU16());

    public float ReadFloat()
    {
        Span<byte> bytes = stackalloc byte[4];
        ReadBytes(bytes);
        return BitConverter.ToSingle(bytes);
    }

    public double ReadDouble()
    {
        Span<byte> bytes = stackalloc byte[8];
        ReadBytes(bytes);
        return BitConverter.ToDouble(bytes);
    }

    public ulong ReadVaruintRaw()
    {
        ulong value = 0UL;
        int shift = 0;
        while (true)
        {
            byte b = ReadU8();
            value |= (ulong)(b & 0x7F) << shift;
            if ((b & 0x80) == 0)
            {
                return value;
            }

            shift += 7;
            if (shift > 63)
            {
                throw new FormatException("Invalid varuint payload.");
            }
        }
    }

    public ulong ReadVaruint() => unchecked((ulong)ReadVarint());

    public long ReadVarint()
    {
        ulong zig = ReadVaruintRaw();
        long value = (long)(zig >> 1);
        if ((zig & 1UL) != 0UL)
        {
            value = -value;
        }

        return value;
    }

    public string ReadString()
    {
        int length = checked((int)ReadVaruint());
        EnsureAvailable(length);
        string text = Encoding.UTF8.GetString(_data.Span.Slice(_offset, length));
        _offset += length;
        return text;
    }

    public byte[] ReadBuffer()
    {
        int length = checked((int)ReadVaruint());
        EnsureAvailable(length);
        byte[] bytes = _data.Slice(_offset, length).ToArray();
        _offset += length;
        return bytes;
    }

    private void ReadBytes(Span<byte> destination)
    {
        EnsureAvailable(destination.Length);
        _data.Span.Slice(_offset, destination.Length).CopyTo(destination);
        _offset += destination.Length;
    }

    private void EnsureAvailable(int bytesRequested)
    {
        if (_offset + bytesRequested > _data.Length)
        {
            throw new IndexOutOfRangeException("Unexpected end of binary payload.");
        }
    }
}
