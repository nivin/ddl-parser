package com.gigaspaces.sql;

import com.gigaspaces.internal.io.BootIOUtils;
import com.gigaspaces.metadata.SpacePropertyDescriptor;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.metadata.SpaceTypeDescriptorBuilder;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;

public class DdlTestCase {
    @Test
    public void demo() throws IOException {
        DdlParser parser = new DdlParser();
        Collection<SpaceTypeDescriptorBuilder> result = parser.parse(BootIOUtils.getResourcePath("ddl-example.txt"));
        System.out.println("Parsed types: " + result.size());
        for (SpaceTypeDescriptorBuilder builder : result) {
            SpaceTypeDescriptor typeDescriptor = builder.create();
            System.out.println(toString(typeDescriptor));
        }
    }

    private String toString(SpaceTypeDescriptor typeDescriptor) {
        StringBuilder sb = new StringBuilder("Type: " + typeDescriptor.getTypeName());
        int numOfFixedProperties = typeDescriptor.getNumOfFixedProperties();
        for (int i = 0; i < numOfFixedProperties; i++) {
            SpacePropertyDescriptor property = typeDescriptor.getFixedProperty(i);
            sb.append(System.lineSeparator()).append("  Property: " + property.getName() + " [" + property.getTypeName() + "]");
        }
        return sb.toString();
    }
}
