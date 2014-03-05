package net.openhft.collections;

interface HashPosMultiMap {

    /**
     * Add an entry.  Allow duplicate hashes, but not hash/position pairs.
     *
     * @param hash   to add
     * @param pos to add
     */
    void put(int hash, int pos);

    /**
     * Remove a hash/pos pair.
     *
     * @param hash   to remove
     * @param pos to remove
     * @return whether a match was found.
     */
    boolean remove(int hash, int pos);

    /**
     * @return normalized hash value, better to be used in subsequent calls
     */
    int startSearch(int hash);

    /**
     * @return normalized hash value, better to be used in subsequent calls
     */
    int startSearch(long hash);

    /**
     * @return the next position for the last search or negative value
     */
    int nextPos();

    void clear();
}
