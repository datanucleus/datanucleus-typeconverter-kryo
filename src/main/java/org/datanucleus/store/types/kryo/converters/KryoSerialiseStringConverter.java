/**********************************************************************
Copyright (c) 2013 Andy Jefferson and others. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors:
    ...
**********************************************************************/
package org.datanucleus.store.types.kryo.converters;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Serializable;

import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.types.converters.TypeConverter;
import org.datanucleus.util.Base64;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Convenience class to handle Kryo serialisation of a Serializable object to/from String.
 * Of use where a datastore allows persistence of String types, but not the type of the Object.
 * Note that we also do Base64 encoding here
 */
public class KryoSerialiseStringConverter implements TypeConverter<Serializable, String>
{
    ThreadLocal kryo = new ThreadLocal();

    public Kryo getKryo()
    {
        Object value = this.kryo.get();
        if (value == null)
        {
            value = new Kryo();
            this.kryo.set(value);
        }
        return (Kryo)value;
    }

    public String toDatastoreType(Serializable memberValue)
    {
        if (memberValue == null)
        {
            return null;
        }

        Kryo kryo = getKryo();
        String str = null;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Output output = null;
        try
        {
            output = new Output(baos);
            kryo.writeClassAndObject(output, memberValue);
        }
        finally
        {
            output.close();
            str = new String(Base64.encode(baos.toByteArray()));
        }

        return str;
    }

    public Serializable toMemberType(String datastoreValue)
    {
        if (datastoreValue == null)
        {
            return null;
        }

        Kryo kryo = getKryo();
        byte[] bytes = Base64.decode(datastoreValue);
        Object obj = null;
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        try
        {
            Input input = null;
            try
            {
                input = new Input(bais);
                obj = kryo.readClassAndObject(input);
            }
            finally
            {
                try
                {
                    bais.close();
                }
                finally
                {
                    if (input != null)
                    {
                        input.close();
                    }
                }
            }
        }
        catch (Exception e)
        {
            throw new NucleusException("Error Kryo deserialising " + datastoreValue, e);
        }
        return (Serializable)obj;
    }
}