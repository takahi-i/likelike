package test.org.unigram.likelike.common;

import org.unigram.likelike.common.SeedClusterId;

import junit.framework.TestCase;

public class TestSeedClusterId extends TestCase {

    public TestSeedClusterId(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        super.setUp();
    }

    public void testHashCode() {
        SeedClusterId sc1 = new SeedClusterId(13232L, 23323L);
        SeedClusterId sc2 = new SeedClusterId(13232L, 23323L); // identical
        assertTrue(sc1.hashCode() == sc2.hashCode());
        
        sc1 = new SeedClusterId(13233L, 23323L);
        sc2 = new SeedClusterId(13232L, 23323L); // different seed
        assertFalse(sc1.hashCode() == sc2.hashCode());
        
        sc1 = new SeedClusterId(13231L, 23323L);
        sc2 = new SeedClusterId(13232L, 23323L); // different seed
        assertFalse(sc1.hashCode() == sc2.hashCode());
        
        sc1 = new SeedClusterId(13232L, 23324L);
        sc2 = new SeedClusterId(13232L, 23323L); // different clusterId
        assertFalse(sc1.hashCode() == sc2.hashCode());
        
        sc1 = new SeedClusterId(13232L, 23323L);
        sc2 = new SeedClusterId(13232L, 23324L); // different clusterId
        assertFalse(sc1.hashCode() == sc2.hashCode());
        
        /* TODO more systematic test */ 
    }

    public void testCompareTo() {
        SeedClusterId sc1 = new SeedClusterId(13232L, 23323L);
        SeedClusterId sc2 = new SeedClusterId(13232L, 23323L); // identical
        assertTrue(sc1.compareTo(sc2) == 0);
        
        sc1 = new SeedClusterId(13233L, 23323L);
        sc2 = new SeedClusterId(13232L, 23323L); // different seed
        assertTrue(sc1.compareTo(sc2) == 1);
        
        sc1 = new SeedClusterId(13231L, 23323L);
        sc2 = new SeedClusterId(13232L, 23323L); // different seed
        assertTrue(sc1.compareTo(sc2) == -1);
        
        sc1 = new SeedClusterId(13232L, 23324L);
        sc2 = new SeedClusterId(13232L, 23323L); // different clusterId
        assertTrue(sc1.compareTo(sc2) == 1);
        
        sc1 = new SeedClusterId(13232L, 23323L);
        sc2 = new SeedClusterId(13232L, 23324L); // different clusterId
        assertTrue(sc1.compareTo(sc2) == -1);
        
    }

    public void testEqualsObject() {
        
        SeedClusterId sc1 = new SeedClusterId(13232L, 23323L);
        SeedClusterId sc2 = new SeedClusterId(13232L, 23323L);
        assertTrue(sc1.equals(sc2));
        assertTrue(sc1.equals(sc1));
        assertTrue(sc2.equals(sc1));
        assertTrue(sc2.equals(sc2));
        
        sc1 = new SeedClusterId(13232L, 23323L);
        sc2 = new SeedClusterId(13233L, 23323L); // different seed
        assertFalse(sc1.equals(sc2));
        assertTrue(sc1.equals(sc1));
        assertFalse(sc2.equals(sc1));
        assertTrue(sc2.equals(sc2));
        
        sc1 = new SeedClusterId(13232L, 23323L);
        sc2 = new SeedClusterId(13232L, 23321L); // different clusterID
        assertFalse(sc1.equals(sc2));
        assertTrue(sc1.equals(sc1));
        assertFalse(sc2.equals(sc1));
        assertTrue(sc2.equals(sc2));
    }

}
