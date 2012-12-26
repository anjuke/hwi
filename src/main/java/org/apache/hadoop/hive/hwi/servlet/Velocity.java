package org.apache.hadoop.hive.hwi.servlet;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;

import javax.servlet.ServletConfig;
import javax.servlet.http.HttpServletRequest;

import org.apache.velocity.Template;
import org.apache.velocity.tools.view.VelocityView;
import org.apache.velocity.context.Context;
import org.apache.velocity.exception.MethodInvocationException;

public class Velocity extends VelocityView{

	/**
	 * The velocity.properties key for specifying the servlet's error template.
	 */
	public static final String PROPERTY_ERROR_TEMPLATE = "tools.view.servlet.error.template";

	/**
	 * The velocity.properties key for specifying the relative directory holding
	 * layout templates.
	 */
	public static final String PROPERTY_LAYOUT_DIR = "tools.view.servlet.layout.directory";

	/**
	 * The velocity.properties key for specifying the servlet's default layout
	 * template's filename.
	 */
	public static final String PROPERTY_DEFAULT_LAYOUT = "tools.view.servlet.layout.default.template";

	/**
	 * The default error template's filename.
	 */
	public static final String DEFAULT_ERROR_TEMPLATE = "Error.vm";

	/**
	 * The default layout directory
	 */
	public static final String DEFAULT_LAYOUT_DIR = "layout/";

	/**
	 * The default filename for the servlet's default layout
	 */
	public static final String DEFAULT_DEFAULT_LAYOUT = "Default.vm";

	/**
	 * The context key that will hold the content of the screen.
	 * 
	 * This key ($screen_content) must be present in the layout template for the
	 * current screen to be rendered.
	 */
	public static final String KEY_SCREEN_CONTENT = "screen_content";

	/**
	 * The context/parameter key used to specify an alternate layout to be used
	 * for a request instead of the default layout.
	 */
	public static final String KEY_LAYOUT = "layout";

	/**
	 * The context key that holds the {@link Throwable} that broke the rendering
	 * of the requested screen.
	 */
	public static final String KEY_ERROR_CAUSE = "error_cause";

	/**
	 * The context key that holds the stack trace of the error that broke the
	 * rendering of the requested screen.
	 */
	public static final String KEY_ERROR_STACKTRACE = "stack_trace";

	/**
	 * The context key that holds the {@link MethodInvocationException} that
	 * broke the rendering of the requested screen.
	 * 
	 * If this value is placed in the context, then $error_cause will hold the
	 * error that this invocation exception is wrapping.
	 */
	public static final String KEY_ERROR_INVOCATION_EXCEPTION = "invocation_exception";

	protected String errorTemplate;
	protected String layoutDir;
	protected String defaultLayout;

	public Velocity(ServletConfig config) {
		super(config);
		
		// check for default template path overrides
		errorTemplate = getProperty(PROPERTY_ERROR_TEMPLATE,
				DEFAULT_ERROR_TEMPLATE);
		layoutDir = getProperty(PROPERTY_LAYOUT_DIR, DEFAULT_LAYOUT_DIR);
		defaultLayout = getProperty(PROPERTY_DEFAULT_LAYOUT,
				DEFAULT_DEFAULT_LAYOUT);

		// preventive error checking! directory must end in /
		if (!layoutDir.endsWith("/")) {
			layoutDir += '/';
		}

		// for efficiency's sake, make defaultLayout a full path now
		defaultLayout = layoutDir + defaultLayout;
	}

	public void render(String path, HttpServletRequest request, Writer writer)
			throws IOException {

		// then get a context
		Context context = createContext(request, null);

		// get the template
		Template template = getTemplate(path);

		// merge the template and context into the response
		mergeTemplate(template, context, writer);
	}

	/**
	 * Overrides VelocityViewServlet.mergeTemplate to do a two-pass render for
	 * handling layouts
	 */
	protected void mergeTemplate(Template template, Context context,
			Writer writer) throws IOException {
		//
		// this section is based on Tim Colson's "two pass render"
		//
		// Render the screen content
		StringWriter sw = new StringWriter();
		template.merge(context, sw);
		// Add the resulting content to the context
		context.put(KEY_SCREEN_CONTENT, sw.toString());

		//Get layout
		Object obj = context.get(KEY_LAYOUT);
		String layout = (obj == null) ? null : obj.toString();
		if (layout == null) {
			layout = defaultLayout;
		} else {
			layout = layoutDir + layout;
		}

		//Render layout
		try {
			template = getTemplate(layout);
		} catch (Exception e) {
			if (!layout.equals(defaultLayout)) {
				template = getTemplate(defaultLayout);
			}
		}
		merge(template, context, writer);
	}

}
