package scray.common.properties;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulation of Scray's property handling
 * 
 * @author Christian Zirpins, Andreas Petter
 */
public class ScrayProperties {

	private static Logger logger = LoggerFactory.getLogger(ScrayProperties.class);
	
	/**
	 * registered properties
	 */
	private static Map<String, Property<?, ?>> properties = new HashMap<String, Property<?, ?>>();

	/**
	 * the order of the stores is important: properties may exist in different
	 * stores. Stores further down the list can hold defaults for those not
	 * providing the properties. Therefore we use a Stack to reverse order the
	 * holders
	 */
	private static Stack<PropertyStorage> stores = new Stack<PropertyStorage>();

	/**
	 * Properties need to be registered before any use is allowed. Properties
	 * may only be registered once.
	 * 
	 * @param prop
	 * @throws PropertyException
	 */
	public static synchronized <T, U> void registerProperty(Property<T, U> prop)
			throws PropertyException {
		ScrayProperties.checkPhase(Phase.register);
		if (properties.get(prop) != null) {
			throw new PropertyExistsException(prop.getName());
		} else {
			properties.put(prop.getName(), prop);
		}
	}

	/**
	 * retrieve the list of all properties.
	 * 
	 * @return
	 */
	public static synchronized Collection<Property<?, ?>> getRegisteredProperties() {
		return properties.values();
	}

	/**
	 * Retrieve a single property value by name.
	 * 
	 * @param prop
	 * @return
	 * @throws PropertyException
	 */
	public static synchronized <T, U> U getPropertyValue(String propName) {
		@SuppressWarnings("unchecked")
		Property<T, U> prop = (Property<T, U>) properties.get(propName);
		if (prop == null) {
			throw new RuntimeException(new PropertyMissingException(propName));
		}
		return getPropertyValue(prop);
	}

	/**
	 * Retrieve a single property value.
	 * 
	 * @param prop
	 * @return
	 * @throws PropertyException
	 */
	public static synchronized <T, U> U getPropertyValue(Property<T, U> prop) {
		U result = null;
		try {
			if (properties.get(prop.getName()) == null) {
				throw new PropertyMissingException(prop.getName());
			}
			ScrayProperties.checkPhase(Phase.use, prop);
			boolean found = false;
			for (int i = stores.size() - 1; i >= 0; i--) {
				PropertyStorage values = stores.get(i);
				if (values.get(prop) != null) {
					result = prop.transformToResult(values.get(prop));
					logger.debug("Found property value for {} in store {}, value is {}", prop.getName(), values.toString(), result);
					found = true;
					break;
				} 
			}
			if (!found) {
				if (prop.hasDefault()) {
					result = prop.getDefault();
					logger.debug("Using default property for {}, value is {}", prop.getName(), result);
				} else {			
					throw new PropertyEmptyException(prop.getName());
				}
			}
		} catch (PropertyException pe) {
			log.error("Property error: ", pe);
			throw new RuntimeException(pe);
		}
		return result;
	}

	/**
	 * we always set the value at the top of the stack
	 * 
	 * @param prop
	 * @param value
	 * @throws PropertyException
	 */
	private static synchronized <T, U> void setPropertyValue(
			Property<T, U> prop, U value) throws PropertyException {
		T storageVal = prop.transformToStorageFormat(value);
		PropertyStorage store = stores.get(stores.size() - 1);
		if (prop.checkConstraints(storageVal)) {
			if (store instanceof PropertyStoragePuttable) {
				((PropertyStoragePuttable) store).put(prop, storageVal);
			}
		} else {
			throw new PropertyConstraintViolatedException(prop.getName());
		}
	}

	/**
	 * Sets a property value by Property.
	 * 
	 * @param prop
	 * @param value
	 * @throws PropertyException
	 */
	public static synchronized <T, U> void setPropertyValue(
			Property<T, U> prop, T value, boolean overwriteIfExists)
			throws PropertyException {
		setPropertyValue(prop.getName(), value, overwriteIfExists);
	}

	/**
	 * Sets a property value by name.
	 * 
	 * @param prop
	 * @param value
	 * @throws PropertyException
	 */
	@SuppressWarnings("unchecked")
	public static synchronized <T, U> void setPropertyValue(String propName,
			U value, boolean overwriteIfExists) throws PropertyException {
		PropertyStorage store = stores.get(stores.size() - 1);
		if (store == null || !(store instanceof PropertyStoragePuttable)) {
			throw new UnsupportedOperationException("Property storage "
					+ ((store != null) ? store.getClass().getName() : "null")
					+ " does not support setting property values!");
		}
		if (properties.get(propName) != null) {
			Property<T, U> prop = (Property<T, U>) properties.get(propName);
			ScrayProperties.checkPhase(Phase.use, prop);
			if (overwriteIfExists) {
				setPropertyValue(prop, value);
			} else {
				if (store.get(prop) != null) {
					throw new PropertyValueExistsException(propName);
				} else {
					setPropertyValue(prop, value);
				}
			}
		} else {
			throw new PropertyMissingException(propName);
		}
	}

	private static Logger log = LoggerFactory.getLogger(ScrayProperties.class);

	public synchronized static void addPropertiesStore(PropertyStorage store)
			throws PropertyException {
		checkPhase(Phase.config);
		log.debug("Adding new ScrayProperties of type "
				+ store.getClass().getName());
		store.init();
		stores.push(store);
	}

	public synchronized Stack<PropertyStorage> getPropertyStores() {
		Stack<PropertyStorage> result = new Stack<PropertyStorage>();
		result.addAll(stores);
		return result;
	}

	/**
	 * Phase handling
	 * 
	 * @return
	 */
	public static enum Phase {
		register(0), config(1), use(2);
		private int num;

		private Phase(int num) {
			this.num = num;
		}

		protected int getNum() {
			return num;
		}
	}

	private static Phase currentPhase = Phase.register;

	public synchronized static Phase getPhase() {
		return currentPhase;
	}

	public synchronized static void checkPhase(Phase phase)
			throws PropertyException {
		checkPhase(phase, null);
	}

	private synchronized static void checkPhase(Phase phase, Property<?, ?> prop)
			throws PropertyException {
		if (phase.getNum() != currentPhase.getNum()) {
			if (prop != null) {
				throw new PropertyPhaseException(phase, prop);
			} else {
				throw new PropertyPhaseException(phase);
			}
		}
	}

	public synchronized static void setPhase(Phase phase)
			throws PropertyException {
		if (phase.getNum() <= currentPhase.getNum()) {
			throw new PropertyPhaseException(phase, currentPhase);
		}
		currentPhase = phase;
		if (phase == Phase.use) {
			// check that all registered Properties have values or defaults
			Collection<Property<?, ?>> props = getRegisteredProperties();
			for (Property<?, ?> property : props) {
				try {
					getPropertyValue(property);
				} catch (RuntimeException e) {
					log.error(
							"Not all properties have values in the configuration or a reasonable default! Found missing property value for property " + property.getName() + ". Check your config for typos!",
							e);
					throw e;
				}
			}
		}
		if (phase == Phase.config) {

			// add the default properties if any
			if ((System.getProperty(PropertyFileStorage.DEFAULT_JVM_ARGUMENT) != null) 
					|| (System.getenv(PropertyFileStorage.DEFAULT_JVM_ARGUMENT) != null)) {
				ScrayProperties
						.addPropertiesStore(new PropertyFileStorage(
								PropertyFileStorage.DEFAULT_JVM_ARGUMENT,
								PropertyFileStorage.FileLocationTypes.LocalPropertiesFile));
			} else if (ScrayProperties.class.getClassLoader().getResource(
					PropertyFileStorage.DEFAULT_PROPERTY_FILE) != null) {
				ScrayProperties
						.addPropertiesStore(new PropertyFileStorage(
								PropertyFileStorage.DEFAULT_PROPERTY_FILE,
								PropertyFileStorage.FileLocationTypes.JarPropertiesFile));
			}

		}
		log.debug("Leaving configuration phase " + currentPhase.toString()
				+ " and transitioning to phase " + phase.toString()
				+ " complete.");
	}

	/**
	 * Reset the registered properties to no available property. Useful for tests.
	 */
	public static void reset() {
		properties.clear();
		stores.clear();
		currentPhase = Phase.register;
	}
	
	
}
