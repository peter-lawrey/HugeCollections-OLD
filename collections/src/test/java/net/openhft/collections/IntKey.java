package net.openhft.collections;

import net.openhft.lang.io.Bytes;

public final class IntKey implements HugeMapKey<IntKey> {
	private int value;
	
	public IntKey() {}
	
	private IntKey(int value) {
		this.value = value;
	}
	
	public IntKey set(int value) {
		this.value = value;
		return this;
	}

	@Override
	public void readMarshallable(Bytes in) throws IllegalStateException {
		value = in.readInt();
	}

	@Override
	public void writeMarshallable(Bytes out) {
		out.writeInt(value);
	}

	@Override
	public IntKey createKeyForPut() {
		return new IntKey(value);
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == null || obj.getClass() != getClass()) return false;
		return value == ((IntKey)obj).value;
	}
	
	@Override
	public int hashCode() {
		return value;
	}
}
